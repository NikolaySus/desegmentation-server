package main

import (
    "context"
    "io"
    "log"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "time"
    kafka "github.com/segmentio/kafka-go"
    "github.com/gin-gonic/gin"
)

type ProducerService struct {
    conn *kafka.Conn
}

func New(kafkaURL string, topic string, partition int) (*ProducerService, error) {
    var err error
    s := ProducerService{}
    s.conn, err = kafka.DialLeader(context.Background(), "tcp", kafkaURL, topic, partition)
    if err != nil {
        return nil, err
    }
    return &s, nil
}

func (s *ProducerService) Transfer(c *gin.Context) {
    data, err := io.ReadAll(c.Request.Body)
    if err != nil {
        log.Fatal("failed to read message:", err)
        c.AbortWithStatus(http.StatusInternalServerError)
        return
    }
    log.Println("incoming segmet: ", string(data))
    s.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
    _, err = s.conn.WriteMessages(
        kafka.Message{Value: []byte(data)},
    )
    if err != nil {
        log.Fatal("failed to write message:", err)
        c.AbortWithStatus(http.StatusInternalServerError)
        return
    }
    c.Status(http.StatusOK)
}

func main() {
    kafkaURL := os.Getenv("kafkaURL")
    topic := os.Getenv("topic")
    port := os.Getenv("port")
    for i := 3; i > 0; i-- {
        log.Print("producer sleeping for ", i, " seconds")
        time.Sleep(1 * time.Second)
    }
    log.Print("producer starting at :", port, " with kafkaURL=", kafkaURL, " and topic=", topic)
    partition := 0
    s, err := New(kafkaURL, topic, partition)
    if err != nil {
        log.Fatal("failed to dial leader:", err)
        return
    }
    r := gin.New()
    r.POST("/transfer", s.Transfer)
    srv := &http.Server{
        Addr: ":" + port,
        Handler: r.Handler(),
    }
    go func() {
        log.Println("producer service started")
        if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            log.Fatalf("listen: %s\n", err)
        }
    }()
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    <-quit
    log.Println("shutdown producer service ...")
    s.conn.DeleteTopics(topic)
    if err := s.conn.Close(); err != nil {
        log.Fatal("failed to close writer:", err)
    }
    ctx, cancel := context.WithTimeout(context.Background(), 1 * time.Second)
    defer cancel()
    if err := srv.Shutdown(ctx); err != nil {
        log.Fatal("producer service shutdown:", err)
    }
    select {
    case <-ctx.Done():
        log.Println("shutdown timeout has expired")
    }
    log.Println("producer service exiting")
}
