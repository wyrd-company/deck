package client

import (
	"github.com/wyrd-company/deck/internal/config"
	"testing"
)

func TestAllowedPolicy(t *testing.T) {
	cases := []struct {
		name, bot   string
		allow, deny []string
		want        bool
	}{
		{name: "absent allow", bot: "helper", want: true},
		{name: "empty allow", bot: "helper", allow: []string{}, want: false},
		{name: "qualified deny on botless target", deny: []string{"helper/2"}, want: false},
		{name: "botless deny", bot: "helper", deny: []string{"2"}, want: false},
		{name: "different explicit bot", bot: "other", deny: []string{"helper/2"}, want: true},
		{name: "deny wins", bot: "helper", allow: []string{"2"}, deny: []string{"helper/2"}, want: false},
		{name: "qualified allow", bot: "helper", allow: []string{"helper/2"}, want: true},
		{name: "qualified allow fails closed on unknown bot", allow: []string{"helper/2"}, want: false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Client{Channels: config.Channels{Allow: tt.allow, Deny: tt.deny}}
			if got := Allowed(cfg, tt.bot, 2); got != tt.want {
				t.Fatalf("Allowed=%v want %v", got, tt.want)
			}
		})
	}
}
