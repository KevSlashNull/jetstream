package client

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/pkg/models"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type noopScheduler struct{}

func (*noopScheduler) AddWork(ctx context.Context, repo string, evt *models.Event) error { return nil }
func (*noopScheduler) Shutdown()                                                         {}

func TestClient_ExitsWhenContextIsCanceled(t *testing.T) {
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	t.Cleanup(func() { listener.Close() })

	go func() {
		err := http.Serve(listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := upgrader.Upgrade(w, r, nil)
			require.NoError(t, err)
			ch := make(chan struct{})
			t.Cleanup(func() { close(ch) })
			<-ch
		}))
		require.NoError(t, err)
	}()

	client, err := NewClient(&ClientConfig{WebsocketURL: "ws://" + listener.Addr().String()}, slog.New(slog.DiscardHandler), &noopScheduler{})
	require.NoError(t, err)
	var cursor int64
	ctx, cancel := context.WithCancel(context.Background())
	waitForClientExit := make(chan struct{})
	go func() {
		err = client.ConnectAndRead(ctx, &cursor)
		require.NoError(t, err)
		waitForClientExit <- struct{}{}
	}()
	time.Sleep(time.Millisecond * 10)
	cancel()

	select {
	case <-time.After(time.Millisecond * 500):
		require.FailNow(t, "timed out waiting for client to exit")
	case <-waitForClientExit:
	}
}
