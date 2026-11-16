package discord

import (
	d "github.com/disgoorg/disgo/discord"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/service/registry"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func Activity(m *d.Message, e registry.Entry, thread uint64) *pb.Activity {
	when := m.CreatedAt
	if when.IsZero() {
		when = m.ID.Time()
	}
	a := &pb.Activity{Id: uint64(m.ID), Time: timestamppb.New(when), Bot: e.Bot, ServerId: e.ServerID, ChannelId: e.ChannelID, ThreadId: thread, Author: &pb.User{Id: uint64(m.Author.ID), Username: m.Author.Username, Bot: m.Author.Bot}}
	if m.Author.GlobalName != nil {
		a.Author.DisplayName = *m.Author.GlobalName
	}
	if m.WebhookID != nil {
		a.Author.WebhookId = uint64(*m.WebhookID)
	}
	msg := &pb.Message{Type: uint32(m.Type), Content: m.Content, Embeds: structs(m.Embeds), Components: structs(m.Components)}
	for _, f := range m.Attachments {
		msg.Attachments = append(msg.Attachments, &pb.Attachment{Id: uint64(f.ID), Filename: f.Filename, ContentType: text(f.ContentType), Size: uint64(f.Size), Url: f.URL, Description: text(f.Description)})
	}
	if m.MessageReference != nil && m.MessageReference.MessageID != nil {
		msg.ReplyToMessageId = uint64(*m.MessageReference.MessageID)
	}
	for _, u := range m.Mentions {
		msg.MentionUserIds = append(msg.MentionUserIds, uint64(u.ID))
	}
	for _, id := range m.MentionRoles {
		msg.MentionRoleIds = append(msg.MentionRoleIds, uint64(id))
	}
	if p := m.Poll; p != nil {
		msg.Poll = &pb.Poll{AllowMultiselect: p.AllowMultiselect}
		if p.Question.Text != nil {
			msg.Poll.Question = *p.Question.Text
		}
		if p.Expiry != nil {
			msg.Poll.ExpiresAt = timestamppb.New(*p.Expiry)
		}
		for _, answer := range p.Answers {
			v := &pb.PollAnswer{}
			if answer.AnswerID != nil {
				v.AnswerId = uint32(*answer.AnswerID)
			}
			if answer.PollMedia.Text != nil {
				v.Text = *answer.PollMedia.Text
			}
			msg.Poll.Answers = append(msg.Poll.Answers, v)
		}
	}
	a.Kind = &pb.Activity_Message{Message: msg}
	return a
}

func text(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
