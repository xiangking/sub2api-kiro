package kiro

const (
	DefaultRegion = "us-east-1"

	AuthMethodSocial         = "social"
	AuthMethodBuilderID      = "builder-id"
	AuthMethodIdentityCenter = "IdC"

	AuthServiceEndpointTemplate = "https://prod.%s.auth.desktop.kiro.dev"
	RefreshURLTemplate          = "https://prod.%s.auth.desktop.kiro.dev/refreshToken"
	CodeWhispererURLTmpl        = "https://codewhisperer.%s.amazonaws.com/generateAssistantResponse"
	AmazonQURLTmpl              = "https://codewhisperer.%s.amazonaws.com/SendMessageStreaming"
	UsageLimitsURLTemplate      = "https://q.%s.amazonaws.com/getUsageLimits"
	BuilderIDStartURL           = "https://view.awsapps.com/start"

	DefaultModelName   = "claude-sonnet-4-5"
	DefaultHealthModel = "claude-haiku-4-5"
	OriginAIEditor     = "AI_EDITOR"
	ChatTriggerManual  = "MANUAL"
	KiroIDEVersion     = "0.7.5"
	DefaultMaxTokens   = 200000
)

var OAuthScopes = []string{
	"codewhisperer:completions",
	"codewhisperer:analysis",
	"codewhisperer:conversations",
	"codewhisperer:transformations",
	"codewhisperer:taskassist",
}

var Models = []string{
	"claude-opus-4-7",
	"claude-opus-4-7-thinking",
	"claude-opus-4-6",
	"claude-opus-4-6-thinking",
	"claude-sonnet-4-6",
	"claude-opus-4-5",
	"claude-opus-4-5-20251101",
	"claude-haiku-4-5",
	"claude-haiku-4-5-20251001",
	"claude-sonnet-4-5",
	"claude-sonnet-4-5-20250929",
	"claude-sonnet-4-20250514",
	"claude-3-7-sonnet-20250219",
}

var ModelMapping = map[string]string{
	"claude-opus-4-7":            "claude-opus-4.7",
	"claude-opus-4-7-thinking":   "claude-opus-4.7",
	"claude-opus-4-6":            "claude-opus-4.6",
	"claude-opus-4-6-thinking":   "claude-opus-4.6",
	"claude-sonnet-4-6":          "claude-sonnet-4.6",
	"claude-opus-4-5":            "claude-opus-4.5",
	"claude-opus-4-5-20251101":   "claude-opus-4.5",
	"claude-haiku-4-5":           "claude-haiku-4.5",
	"claude-haiku-4-5-20251001":  "claude-haiku-4.5",
	"claude-sonnet-4-5":          "CLAUDE_SONNET_4_5_20250929_V1_0",
	"claude-sonnet-4-5-20250929": "CLAUDE_SONNET_4_5_20250929_V1_0",
	"claude-sonnet-4-20250514":   "CLAUDE_SONNET_4_20250514_V1_0",
	"claude-3-7-sonnet-20250219": "CLAUDE_3_7_SONNET_20250219_V1_0",
}

func IsKnownModel(model string) bool {
	for _, m := range Models {
		if m == model {
			return true
		}
	}
	_, ok := ModelMapping[model]
	return ok
}

func MapModel(model string) string {
	if mapped := ModelMapping[model]; mapped != "" {
		return mapped
	}
	if model != "" {
		return model
	}
	return ModelMapping[DefaultModelName]
}
