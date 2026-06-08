// Package consumer subscribes the user service to auth's lifecycle events and
// keeps the profile store in sync. Handlers are idempotent (at-least-once
// delivery): UserRegistered upserts a profile, UserDeleted drops it.
package consumer

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	"github.com/malvinpratama/iam-go-libs/events"
	"github.com/malvinpratama/iam-go-user/internal/db"
)

// Consumer binds JetStream subscriptions to the profile queries.
type Consumer struct {
	q   *db.Queries
	js  nats.JetStreamContext
	log *slog.Logger
}

// New builds a Consumer.
func New(q *db.Queries, js nats.JetStreamContext, log *slog.Logger) *Consumer {
	return &Consumer{q: q, js: js, log: log}
}

// Start registers durable push subscriptions with manual ack. The returned
// subscriptions are drained when ctx is cancelled.
func (c *Consumer) Start(ctx context.Context) error {
	regSub, err := c.js.Subscribe(events.SubjectUserRegistered, c.handleRegistered,
		nats.Durable("user-service-registered"), nats.ManualAck(), nats.AckExplicit())
	if err != nil {
		return err
	}
	delSub, err := c.js.Subscribe(events.SubjectUserDeleted, c.handleDeleted,
		nats.Durable("user-service-deleted"), nats.ManualAck(), nats.AckExplicit())
	if err != nil {
		_ = regSub.Drain()
		return err
	}
	go func() {
		<-ctx.Done()
		_ = regSub.Drain()
		_ = delSub.Drain()
	}()
	c.log.Info("event consumer started")
	return nil
}

func (c *Consumer) handleRegistered(m *nats.Msg) {
	var ev events.UserRegistered
	if err := json.Unmarshal(m.Data, &ev); err != nil {
		c.log.Warn("bad UserRegistered payload", "err", err)
		_ = m.Term() // poison message — don't redeliver
		return
	}
	uid, err := uuid.Parse(ev.UserID)
	if err != nil {
		c.log.Warn("bad user_id in UserRegistered", "err", err)
		_ = m.Term()
		return
	}
	if err := c.q.UpsertProfile(context.Background(), db.UpsertProfileParams{
		UserID: uid, DisplayName: ev.DisplayName,
	}); err != nil {
		c.log.Warn("upsert profile failed; will retry", "err", err)
		_ = m.Nak() // redeliver later
		return
	}
	_ = m.Ack()
	c.log.Info("profile created from event", "user_id", ev.UserID)
}

func (c *Consumer) handleDeleted(m *nats.Msg) {
	var ev events.UserDeleted
	if err := json.Unmarshal(m.Data, &ev); err != nil {
		c.log.Warn("bad UserDeleted payload", "err", err)
		_ = m.Term()
		return
	}
	uid, err := uuid.Parse(ev.UserID)
	if err != nil {
		c.log.Warn("bad user_id in UserDeleted", "err", err)
		_ = m.Term()
		return
	}
	if err := c.q.DeleteProfile(context.Background(), uid); err != nil {
		c.log.Warn("delete profile failed; will retry", "err", err)
		_ = m.Nak()
		return
	}
	_ = m.Ack()
	c.log.Info("profile deleted from event", "user_id", ev.UserID)
}
