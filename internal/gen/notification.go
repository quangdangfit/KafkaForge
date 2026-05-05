// Package gen produces synthetic payloads for traffic generation.
package gen

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"
)

// Notification is a fake push-notification payload.
type Notification struct {
	ID     string    `json:"id"`
	UserID int64     `json:"user_id"`
	Title  string    `json:"title"`
	Body   string    `json:"body"`
	SentAt time.Time `json:"sent_at"`
}

// Generator builds Notifications. It is safe for single-goroutine use; create
// one per goroutine when fanning out.
type Generator struct {
	rng     *rand.Rand
	users   int64
	titles  []string
	bodies  []string
	counter uint64
}

// NewGenerator returns a Generator that draws user IDs from [1, users].
func NewGenerator(seed uint64, users int64) *Generator {
	if users <= 0 {
		users = 1_000_000
	}
	return &Generator{
		rng:   rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15)),
		users: users,
		titles: []string{
			"New message",
			"Order shipped",
			"Payment received",
			"Friend request",
			"Reminder",
			"Promotion",
		},
		bodies: []string{
			"You have a new message waiting.",
			"Your order is on the way.",
			"We received your payment, thank you.",
			"Tap to view details.",
			"Don't forget your appointment tomorrow.",
			"Limited time offer just for you.",
		},
	}
}

// Next returns the next Notification with SentAt set to now (UTC).
func (g *Generator) Next() Notification {
	g.counter++
	return Notification{
		ID:     fmt.Sprintf("n-%d-%d", time.Now().UnixNano(), g.counter),
		UserID: g.rng.Int64N(g.users) + 1,
		Title:  g.titles[g.rng.IntN(len(g.titles))],
		Body:   g.bodies[g.rng.IntN(len(g.bodies))],
		SentAt: time.Now().UTC(),
	}
}

// Marshal serialises n to JSON and returns it together with a partitioning key
// derived from UserID so messages for the same user land on the same partition.
func (n Notification) Marshal() (key, value []byte, err error) {
	v, err := json.Marshal(n)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal notification: %w", err)
	}
	return []byte(fmt.Sprintf("%d", n.UserID)), v, nil
}
