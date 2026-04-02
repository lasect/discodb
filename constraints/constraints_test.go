package constraints

import (
	"strings"
	"testing"
)

func TestValidateMessageContent(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr bool
	}{
		{"empty", "", false},
		{"short", "hello", false},
		{"at limit", strings.Repeat("a", MaxMessageContent), false},
		{"over limit", strings.Repeat("a", MaxMessageContent+1), true},
		{"way over", strings.Repeat("a", MaxMessageContent*2), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateMessageContent(tt.content)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateMessageContent() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateEmbeds(t *testing.T) {
	tests := []struct {
		name    string
		embeds  []Embed
		wantErr bool
	}{
		{"empty", nil, false},
		{"single small", []Embed{{Description: "hello"}}, false},
		{"too many embeds", make([]Embed, MaxEmbedsPerMessage+1), true},
		{
			"total too large",
			[]Embed{
				{Description: strings.Repeat("a", 3000)},
				{Description: strings.Repeat("b", 3001)},
			},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateEmbeds(tt.embeds)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateEmbeds() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEmbedSize(t *testing.T) {
	embed := Embed{
		Title:       "title",
		Description: "description",
		Fields: []Field{
			{Name: "name1", Value: "value1"},
			{Name: "name2", Value: "value2"},
		},
		Footer: &Footer{Text: "footer"},
	}

	size := EmbedSize(embed)
	expected := len("title") + len("description") + len("name1") + len("value1") + len("name2") + len("value2") + len("footer")
	if size != expected {
		t.Errorf("EmbedSize() = %d, want %d", size, expected)
	}
}

func TestValidateRowFits(t *testing.T) {
	tests := []struct {
		name       string
		headerSize int
		bodySize   int
		wantErr    bool
	}{
		{"small row", 50, 100, false},
		{"header too large", MaxRowHeaderEncoded + 1, 100, true},
		{"body too large", 50, MaxRowBodyInline + 1, true},
		{"both at limit", MaxRowHeaderEncoded, MaxRowBodyInline, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRowFits(tt.headerSize, tt.bodySize)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateRowFits() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNeedsOverflow(t *testing.T) {
	tests := []struct {
		size int
		want bool
	}{
		{0, false},
		{100, false},
		{MaxRowBodyInline, false},
		{MaxRowBodyInline + 1, true},
		{MaxRowBodyInline * 2, true},
	}

	for _, tt := range tests {
		got := NeedsOverflow(tt.size)
		if got != tt.want {
			t.Errorf("NeedsOverflow(%d) = %v, want %v", tt.size, got, tt.want)
		}
	}
}

func TestComputeToastChunks(t *testing.T) {
	tests := []struct {
		dataSize int
		want     int
	}{
		{0, 0},
		{100, 1},
		{ToastChunkSize, 1},
		{ToastChunkSize + 1, 2},
		{ToastChunkSize * 3, 3},
		{ToastChunkSize*3 + 1, 4},
	}

	for _, tt := range tests {
		got := ComputeToastChunks(tt.dataSize)
		if got != tt.want {
			t.Errorf("ComputeToastChunks(%d) = %d, want %d", tt.dataSize, got, tt.want)
		}
	}
}

func TestValidateRolesCount(t *testing.T) {
	if err := ValidateRolesCount(0); err != nil {
		t.Error("expected no error for 0 roles")
	}
	if err := ValidateRolesCount(MaxRolesPerGuild); err != nil {
		t.Error("expected no error at limit")
	}
	if err := ValidateRolesCount(MaxRolesPerGuild + 1); err == nil {
		t.Error("expected error over limit")
	}
}

func TestValidateChannelsCount(t *testing.T) {
	if err := ValidateChannelsCount(0); err != nil {
		t.Error("expected no error for 0 channels")
	}
	if err := ValidateChannelsCount(MaxChannelsPerGuild); err != nil {
		t.Error("expected no error at limit")
	}
	if err := ValidateChannelsCount(MaxChannelsPerGuild + 1); err == nil {
		t.Error("expected error over limit")
	}
}

func TestAllLimits(t *testing.T) {
	limits := AllLimits()
	
	// Check key limits are present
	expected := []string{
		"message_content",
		"embeds_per_message",
		"embed_total_chars",
		"roles_per_guild",
		"channels_per_guild",
		"toast_chunk_size",
	}
	
	for _, key := range expected {
		if _, ok := limits[key]; !ok {
			t.Errorf("AllLimits() missing key %q", key)
		}
	}
	
	// Verify values match constants
	if limits["message_content"] != MaxMessageContent {
		t.Errorf("message_content = %d, want %d", limits["message_content"], MaxMessageContent)
	}
}

func TestJSONSize(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  int
	}{
		{"string", "hello", 7}, // "hello" with quotes
		{"int", 123, 3},
		{"bool", true, 4},
		{"nil", nil, 4}, // null
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := JSONSize(tt.value)
			if err != nil {
				t.Fatalf("JSONSize() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("JSONSize() = %d, want %d", got, tt.want)
			}
		})
	}
}
