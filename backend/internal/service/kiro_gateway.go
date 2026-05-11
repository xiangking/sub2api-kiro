package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/Wei-Shaw/sub2api/internal/pkg/kiro"
	"github.com/Wei-Shaw/sub2api/internal/util/responseheaders"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
)

func (s *GatewayService) forwardKiro(ctx context.Context, c *gin.Context, account *Account, parsed *ParsedRequest, startTime time.Time) (*ForwardResult, error) {
	if account == nil || parsed == nil {
		return nil, errors.New("kiro forward: nil account or request")
	}
	req, err := kiroRequestFromAnthropicBody(parsed.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"type": "error", "error": gin.H{"type": "invalid_request_error", "message": "Invalid request body"}})
		return nil, err
	}
	originalModel := req.Model
	if !s.isModelSupportedByAccount(account, originalModel) {
		c.JSON(http.StatusBadRequest, gin.H{"type": "error", "error": gin.H{"type": "invalid_request_error", "message": fmt.Sprintf("model %s is not supported by this Kiro account", originalModel)}})
		return nil, fmt.Errorf("kiro model not supported: %s", originalModel)
	}
	req.Model = account.GetMappedModel(originalModel)
	if strings.TrimSpace(req.Model) == "" {
		req.Model = kiro.DefaultModelName
	}

	creds := kiroCredentialsFromAccount(account)
	client := kiro.NewClient(creds, nil)
	refresh, err := client.EnsureAccessToken(ctx)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"type": "error", "error": gin.H{"type": "upstream_error", "message": "Kiro token refresh failed"}})
		return nil, err
	}
	if refresh.Refreshed {
		persistKiroCredentials(ctx, s.accountRepo, account, refresh.Credentials)
		client = kiro.NewClient(refresh.Credentials, nil)
	}

	upstreamReq, upstreamModel, err := client.BuildHTTPRequest(ctx, req)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"type": "error", "error": gin.H{"type": "upstream_error", "message": "Failed to build Kiro request"}})
		return nil, err
	}

	proxyURL := ""
	if account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	resp, err := s.httpUpstream.DoWithTLS(upstreamReq, proxyURL, account.ID, account.Concurrency, s.tlsFPProfileService.ResolveTLSProfile(account))
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"type": "error", "error": gin.H{"type": "upstream_error", "message": "Kiro upstream request failed"}})
		return nil, err
	}

	if resp.StatusCode == http.StatusUnauthorized && strings.TrimSpace(client.Credentials().RefreshToken) != "" {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 2<<20))
		_ = resp.Body.Close()
		forcedRefresh, refreshErr := client.ForceRefreshAccessToken(ctx)
		if refreshErr != nil {
			c.JSON(http.StatusBadGateway, gin.H{"type": "error", "error": gin.H{"type": "upstream_error", "message": "Kiro token refresh failed"}})
			return nil, refreshErr
		}
		persistKiroCredentials(ctx, s.accountRepo, account, forcedRefresh.Credentials)
		client = kiro.NewClient(forcedRefresh.Credentials, nil)
		upstreamReq, upstreamModel, err = client.BuildHTTPRequest(ctx, req)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"type": "error", "error": gin.H{"type": "upstream_error", "message": "Failed to build Kiro request"}})
			return nil, err
		}
		resp, err = s.httpUpstream.DoWithTLS(upstreamReq, proxyURL, account.ID, account.Concurrency, s.tlsFPProfileService.ResolveTLSProfile(account))
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"type": "error", "error": gin.H{"type": "upstream_error", "message": "Kiro upstream request failed"}})
			return nil, err
		}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		if isKiroContextLimit(resp.StatusCode, body) {
			c.JSON(http.StatusBadRequest, gin.H{"type": "error", "error": gin.H{"type": "invalid_request_error", "message": "prompt is too long: 200001 tokens > 200000 maximum"}})
			return nil, fmt.Errorf("kiro context limit exceeded")
		}
		upstreamStatus := normalizeKiroUpstreamStatus(resp.StatusCode, body)
		if s.shouldFailoverUpstreamError(upstreamStatus) {
			return nil, &UpstreamFailoverError{StatusCode: upstreamStatus, ResponseBody: body}
		}
		if upstreamStatus == http.StatusTooManyRequests {
			c.JSON(http.StatusTooManyRequests, gin.H{"type": "error", "error": gin.H{"type": "rate_limit_error", "message": "Upstream rate limit exceeded, please retry later"}})
			return nil, fmt.Errorf("kiro upstream error: %d", upstreamStatus)
		}
		c.JSON(mapUpstreamStatusCode(upstreamStatus), gin.H{"type": "error", "error": gin.H{"type": "upstream_error", "message": "Kiro upstream request failed"}})
		return nil, fmt.Errorf("kiro upstream error: %d", upstreamStatus)
	}

	if parsed.OnUpstreamAccepted != nil {
		parsed.OnUpstreamAccepted()
	}

	if req.Stream {
		usage, firstTokenMs, clientDisconnect, err := s.handleKiroClaudeStream(ctx, c, resp, originalModel, startTime)
		if err != nil {
			return nil, err
		}
		return &ForwardResult{
			RequestID:        kiroRequestID(resp),
			Usage:            usage,
			Model:            originalModel,
			UpstreamModel:    upstreamModel,
			Stream:           true,
			Duration:         time.Since(startTime),
			FirstTokenMs:     firstTokenMs,
			ClientDisconnect: clientDisconnect,
		}, nil
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	kiroResp := kiro.ParseNonStreamingResponse(body)
	usage := ClaudeUsage{InputTokens: kiroResp.Usage.InputTokens, OutputTokens: kiroResp.Usage.OutputTokens}
	out := apicompat.AnthropicResponse{
		ID:         "msg_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:24],
		Type:       "message",
		Role:       "assistant",
		Model:      originalModel,
		StopReason: defaultString(kiroResp.StopReason, "end_turn"),
		Content: []apicompat.AnthropicContentBlock{{
			Type: "text",
			Text: kiroResp.Content,
		}},
		Usage: apicompat.AnthropicUsage{
			InputTokens:  usage.InputTokens,
			OutputTokens: usage.OutputTokens,
		},
	}
	payload, _ := json.Marshal(out)
	if s.responseHeaderFilter != nil {
		responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	}
	c.Data(http.StatusOK, "application/json", payload)
	return &ForwardResult{
		RequestID:     kiroRequestID(resp),
		Usage:         usage,
		Model:         originalModel,
		UpstreamModel: upstreamModel,
		Stream:        false,
		Duration:      time.Since(startTime),
	}, nil
}

func (s *GatewayService) handleKiroClaudeStream(ctx context.Context, c *gin.Context, resp *http.Response, originalModel string, startTime time.Time) (ClaudeUsage, *int, bool, error) {
	if s.responseHeaderFilter != nil {
		responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	}
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")
	c.Status(http.StatusOK)

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		return ClaudeUsage{}, nil, false, errors.New("streaming not supported")
	}

	messageID := "msg_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:24]
	write := func(event string, data any) bool {
		b, _ := json.Marshal(data)
		if _, err := fmt.Fprintf(c.Writer, "event: %s\ndata: %s\n\n", event, b); err != nil {
			return true
		}
		flusher.Flush()
		return false
	}
	usage := ClaudeUsage{}
	if write("message_start", gin.H{"type": "message_start", "message": gin.H{
		"id": messageID, "type": "message", "role": "assistant", "content": []any{}, "model": originalModel,
		"stop_reason": nil, "stop_sequence": nil, "usage": gin.H{"input_tokens": 0, "output_tokens": 0},
	}}) {
		return usage, nil, true, nil
	}
	if write("content_block_start", gin.H{"type": "content_block_start", "index": 0, "content_block": gin.H{"type": "text", "text": ""}}) {
		return usage, nil, true, nil
	}

	var firstTokenMs *int
	var contentBuilder strings.Builder
	blockIndex := 0
	currentBlockType := "text"
	currentToolUseID := ""
	toolUseSeen := false
	toolBlockOpen := true
	textBlockOpen := true
	openToolBlock := func(tool *kiro.ToolUse) bool {
		if tool == nil {
			return false
		}
		blockIndex++
		currentBlockType = "tool"
		currentToolUseID = tool.ToolUseID
		toolBlockOpen = true
		toolUseSeen = true
		return write("content_block_start", gin.H{
			"type":  "content_block_start",
			"index": blockIndex,
			"content_block": gin.H{
				"type":  "tool_use",
				"id":    tool.ToolUseID,
				"name":  tool.Name,
				"input": gin.H{},
			},
		})
	}
	openTextBlock := func() bool {
		blockIndex++
		currentBlockType = "text"
		currentToolUseID = ""
		textBlockOpen = true
		return write("content_block_start", gin.H{
			"type":  "content_block_start",
			"index": blockIndex,
			"content_block": gin.H{
				"type": "text",
				"text": "",
			},
		})
	}
	stopCurrentBlock := func() bool {
		if currentBlockType == "tool" && toolBlockOpen {
			if write("content_block_stop", gin.H{"type": "content_block_stop", "index": blockIndex}) {
				return true
			}
			toolBlockOpen = false
		}
		if currentBlockType == "text" && textBlockOpen {
			if write("content_block_stop", gin.H{"type": "content_block_stop", "index": blockIndex}) {
				return true
			}
			textBlockOpen = false
		}
		currentBlockType = ""
		currentToolUseID = ""
		return false
	}
	handleEvent := func(event kiro.StreamEvent) bool {
		if firstTokenMs == nil && (event.Type == "content" || event.Type == "toolUse" || event.Type == "toolUseInput" || event.Type == "toolUseStop" || event.Type == "contextUsage") {
			ms := int(time.Since(startTime).Milliseconds())
			firstTokenMs = &ms
		}
		switch event.Type {
		case "content":
			if event.Content == "" {
				return false
			}
			if currentBlockType == "tool" {
				if stopCurrentBlock() {
					return true
				}
				if openTextBlock() {
					return true
				}
			} else if currentBlockType == "" {
				if openTextBlock() {
					return true
				}
			}
			contentBuilder.WriteString(event.Content)
			usage.OutputTokens += (len([]rune(event.Content)) + 3) / 4
			return write("content_block_delta", gin.H{"type": "content_block_delta", "index": blockIndex, "delta": gin.H{"type": "text_delta", "text": event.Content}})
		case "toolUse":
			if event.ToolUse == nil {
				return false
			}
			if currentBlockType == "text" && textBlockOpen {
				if stopCurrentBlock() {
					return true
				}
			}
			if currentBlockType != "tool" || !toolBlockOpen || currentToolUseID != event.ToolUse.ToolUseID {
				if openToolBlock(event.ToolUse) {
					return true
				}
			}
			if event.ToolUse.Input != "" {
				if write("content_block_delta", gin.H{
					"type":  "content_block_delta",
					"index": blockIndex,
					"delta": gin.H{"type": "input_json_delta", "partial_json": event.ToolUse.Input},
				}) {
					return true
				}
			}
			if event.ToolUse.Stop {
				if write("content_block_stop", gin.H{"type": "content_block_stop", "index": blockIndex}) {
					return true
				}
				toolBlockOpen = false
				currentBlockType = ""
				currentToolUseID = ""
			}
			return false
		case "toolUseInput":
			if event.Input == "" {
				return false
			}
			if currentBlockType != "tool" || !toolBlockOpen {
				return false
			}
			return write("content_block_delta", gin.H{
				"type":  "content_block_delta",
				"index": blockIndex,
				"delta": gin.H{"type": "input_json_delta", "partial_json": event.Input},
			})
		case "toolUseStop":
			if currentBlockType != "tool" || !toolBlockOpen {
				return false
			}
			if write("content_block_stop", gin.H{"type": "content_block_stop", "index": blockIndex}) {
				return true
			}
			toolBlockOpen = false
			currentBlockType = ""
			currentToolUseID = ""
			return false
		case "contextUsage":
			return false
		default:
			return false
		}
	}

	if isKiroEventStreamResponse(resp.Header) {
		type streamEvent struct {
			event kiro.StreamEvent
			err   error
		}
		events := make(chan streamEvent, 16)
		done := make(chan struct{})
		sendEvent := func(ev streamEvent) bool {
			select {
			case events <- ev:
				return true
			case <-done:
				return false
			}
		}
		go func() {
			defer close(events)
			decoder := kiro.NewEventStreamDecoder(resp.Body)
			for {
				event, err := decoder.Decode()
				if err != nil {
					if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
						return
					}
					_ = sendEvent(streamEvent{err: err})
					return
				}
				if !sendEvent(streamEvent{event: event}) {
					return
				}
			}
		}()
		defer close(done)

		keepaliveInterval := time.Duration(0)
		if s.cfg != nil && s.cfg.Gateway.StreamKeepaliveInterval > 0 {
			keepaliveInterval = time.Duration(s.cfg.Gateway.StreamKeepaliveInterval) * time.Second
		}
		var keepaliveTicker *time.Ticker
		if keepaliveInterval > 0 {
			keepaliveTicker = time.NewTicker(keepaliveInterval)
			defer keepaliveTicker.Stop()
		}
		var keepaliveCh <-chan time.Time
		if keepaliveTicker != nil {
			keepaliveCh = keepaliveTicker.C
		}
		lastDataAt := time.Now()

		streamDone := false
	streamLoop:
		for {
			select {
			case <-ctx.Done():
				return usage, firstTokenMs, true, nil
			case ev, ok := <-events:
				if !ok {
					streamDone = true
					break streamLoop
				}
				if ev.err != nil {
					return usage, firstTokenMs, false, ev.err
				}
				lastDataAt = time.Now()
				if handleEvent(ev.event) {
					return usage, firstTokenMs, true, nil
				}
			case <-keepaliveCh:
				if keepaliveInterval <= 0 || time.Since(lastDataAt) < keepaliveInterval {
					continue
				}
				if _, err := fmt.Fprint(c.Writer, "event: ping\ndata: {\"type\": \"ping\"}\n\n"); err != nil {
					return usage, firstTokenMs, true, nil
				}
				flusher.Flush()
			}
		}
		if !streamDone {
			return usage, firstTokenMs, false, errors.New("kiro stream ended unexpectedly")
		}
	} else {
		var buffer string
		scanner := bufio.NewScanner(resp.Body)
		maxLineSize := defaultMaxLineSize
		if s.cfg != nil && s.cfg.Gateway.MaxLineSize > 0 {
			maxLineSize = s.cfg.Gateway.MaxLineSize
		}
		scanner.Buffer(make([]byte, 0, 64*1024), maxLineSize)
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return usage, firstTokenMs, true, nil
			default:
			}
			buffer += scanner.Text()
			events, remaining := kiro.ParseEventStreamBuffer(buffer)
			buffer = remaining
			for _, event := range events {
				if handleEvent(event) {
					return usage, firstTokenMs, true, nil
				}
			}
		}
		if err := scanner.Err(); err != nil {
			return usage, firstTokenMs, false, err
		}
	}
	usage.InputTokens = usage.OutputTokens / 2
	stopReason := "end_turn"
	if toolUseSeen {
		stopReason = "tool_use"
	}
	if currentBlockType != "" {
		if write("content_block_stop", gin.H{"type": "content_block_stop", "index": blockIndex}) {
			return usage, firstTokenMs, true, nil
		}
	}
	if write("message_delta", gin.H{"type": "message_delta", "delta": gin.H{"stop_reason": stopReason, "stop_sequence": nil}, "usage": gin.H{"output_tokens": usage.OutputTokens}}) {
		return usage, firstTokenMs, true, nil
	}
	_ = write("message_stop", gin.H{"type": "message_stop"})
	_ = contentBuilder
	return usage, firstTokenMs, false, nil
}

func isKiroEventStreamResponse(header http.Header) bool {
	contentType := strings.ToLower(header.Get("Content-Type"))
	return strings.Contains(contentType, "application/vnd.amazon.eventstream")
}

func (s *GatewayService) forwardKiroAsChatCompletions(ctx context.Context, c *gin.Context, account *Account, body []byte, parsed *ParsedRequest) (*ForwardResult, error) {
	var ccReq apicompat.ChatCompletionsRequest
	if err := json.Unmarshal(body, &ccReq); err != nil {
		return nil, err
	}
	responsesReq, err := apicompat.ChatCompletionsToResponses(&ccReq)
	if err != nil {
		return nil, err
	}
	anthropicReq, err := apicompat.ResponsesToAnthropicRequest(responsesReq)
	if err != nil {
		return nil, err
	}
	anthropicReq.Stream = ccReq.Stream
	anthropicBody, err := json.Marshal(anthropicReq)
	if err != nil {
		return nil, err
	}
	kiroParsed, err := ParseGatewayRequest(anthropicBody, "")
	if err != nil {
		return nil, err
	}
	if parsed != nil {
		kiroParsed.GroupID = parsed.GroupID
	}
	if !ccReq.Stream {
		rec := &responseCaptureWriter{ResponseWriter: c.Writer, header: http.Header{}}
		c.Writer = rec
		result, err := s.forwardKiro(ctx, c, account, kiroParsed, time.Now())
		if err != nil {
			return nil, err
		}
		var anthropicResp apicompat.AnthropicResponse
		if err := json.Unmarshal(rec.body.Bytes(), &anthropicResp); err != nil {
			return nil, err
		}
		ccResp := apicompat.ResponsesToChatCompletions(apicompat.AnthropicToResponsesResponse(&anthropicResp), ccReq.Model)
		payload, _ := json.Marshal(ccResp)
		for k, vals := range rec.header {
			for _, v := range vals {
				c.Writer.Header().Add(k, v)
			}
		}
		c.Data(http.StatusOK, "application/json", payload)
		return result, nil
	}
	// Streaming Chat Completions compatibility is intentionally simple: Kiro is
	// primarily exposed as Claude Messages for Claude Code clients.
	ccReq.Stream = false
	body, _ = json.Marshal(ccReq)
	return s.forwardKiroAsChatCompletions(ctx, c, account, body, parsed)
}

type responseCaptureWriter struct {
	gin.ResponseWriter
	header http.Header
	body   bytes.Buffer
	status int
}

func (w *responseCaptureWriter) Header() http.Header {
	return w.header
}

func (w *responseCaptureWriter) WriteHeader(code int) {
	w.status = code
}

func (w *responseCaptureWriter) Write(data []byte) (int, error) {
	return w.body.Write(data)
}

func (w *responseCaptureWriter) WriteString(s string) (int, error) {
	return w.body.WriteString(s)
}

func kiroRequestFromAnthropicBody(body []byte) (kiro.Request, error) {
	var req struct {
		Model    string          `json:"model"`
		System   json.RawMessage `json:"system"`
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
		Tools       []kiro.Tool `json:"tools"`
		Stream      bool        `json:"stream"`
		MaxTokens   int         `json:"max_tokens"`
		Temperature *float64    `json:"temperature"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return kiro.Request{}, err
	}
	out := kiro.Request{
		Model:       req.Model,
		System:      req.System,
		Tools:       req.Tools,
		Stream:      req.Stream,
		MaxTokens:   req.MaxTokens,
		Temperature: req.Temperature,
	}
	out.Messages = make([]kiro.Message, 0, len(req.Messages))
	for _, msg := range req.Messages {
		out.Messages = append(out.Messages, kiro.Message{Role: msg.Role, Content: msg.Content})
	}
	return out, nil
}

func kiroCredentialsFromAccount(account *Account) kiro.Credentials {
	return kiro.Credentials{
		UUID:                  account.GetCredential("uuid"),
		AccessToken:           firstCredential(account, "access_token", "accessToken"),
		RefreshToken:          firstCredential(account, "refresh_token", "refreshToken"),
		ClientID:              firstCredential(account, "client_id", "clientId"),
		ClientSecret:          firstCredential(account, "client_secret", "clientSecret"),
		ClientSecretExpiresAt: account.GetCredentialAsInt64("client_secret_expires_at"),
		AuthMethod:            firstCredential(account, "auth_method", "authMethod"),
		ExpiresAt:             firstCredential(account, "expires_at", "expiresAt"),
		ProfileARN:            firstCredential(account, "profile_arn", "profileArn"),
		Region:                defaultString(account.GetCredential("region"), kiro.DefaultRegion),
	}
}

func persistKiroCredentials(ctx context.Context, repo AccountRepository, account *Account, creds kiro.Credentials) {
	if account == nil {
		return
	}
	next := cloneCredentials(account.Credentials)
	next["uuid"] = creds.UUID
	next["access_token"] = creds.AccessToken
	next["refresh_token"] = creds.RefreshToken
	next["client_id"] = creds.ClientID
	next["client_secret"] = creds.ClientSecret
	next["client_secret_expires_at"] = creds.ClientSecretExpiresAt
	next["auth_method"] = creds.AuthMethod
	next["expires_at"] = creds.ExpiresAt
	next["profile_arn"] = creds.ProfileARN
	next["region"] = creds.Region
	_ = persistAccountCredentials(ctx, repo, account, next)
}

func firstCredential(account *Account, keys ...string) string {
	for _, key := range keys {
		if v := strings.TrimSpace(account.GetCredential(key)); v != "" {
			return v
		}
	}
	return ""
}

func defaultString(v, fallback string) string {
	if strings.TrimSpace(v) != "" {
		return v
	}
	return fallback
}

func kiroRequestID(resp *http.Response) string {
	if resp == nil {
		return "generated:" + generateRequestID()
	}
	for _, key := range []string{"x-amzn-requestid", "x-amzn-request-id", "x-request-id"} {
		if v := strings.TrimSpace(resp.Header.Get(key)); v != "" {
			return v
		}
	}
	return "generated:" + generateRequestID()
}

func isKiroContextLimit(status int, body []byte) bool {
	if status != 400 {
		return false
	}
	reason := strings.ToUpper(gjson.GetBytes(body, "reason").String())
	message := strings.ToLower(gjson.GetBytes(body, "message").String())
	return reason == "CONTENT_LENGTH_EXCEEDS_THRESHOLD" ||
		strings.Contains(message, "input is too long") ||
		strings.Contains(message, "too long")
}

func normalizeKiroUpstreamStatus(status int, body []byte) int {
	if status == http.StatusTooManyRequests {
		return status
	}
	if status != http.StatusBadRequest {
		return status
	}
	errType := strings.ToLower(gjson.GetBytes(body, "__type").String())
	reason := strings.ToUpper(gjson.GetBytes(body, "reason").String())
	message := strings.ToLower(gjson.GetBytes(body, "message").String())
	if strings.Contains(errType, "throttlingexception") ||
		reason == "INSUFFICIENT_MODEL_CAPACITY" ||
		strings.Contains(message, "high traffic") ||
		strings.Contains(message, "try again shortly") {
		return http.StatusTooManyRequests
	}
	return status
}
