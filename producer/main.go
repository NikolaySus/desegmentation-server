package main

import (
    "context"
    //"fmt"
    "log"
    "os"
    //"strings"
    "time"
    kafka "github.com/segmentio/kafka-go"
)

func main() {
    kafkaURL := os.Getenv("kafkaURL")
    topic := os.Getenv("topic")
    for i := 3; i > 0; i-- {
        log.Print("producer sleeping for ", i, " seconds")
        time.Sleep(1 * time.Second)
    }
    log.Print("producer started with: ", kafkaURL, topic)
    partition := 0
    conn, err := kafka.DialLeader(context.Background(), "tcp", kafkaURL, topic, partition)
    if err != nil {
        log.Fatal("failed to dial leader:", err)
    }

    conn.SetWriteDeadline(time.Now().Add(10*time.Second))
    _, err = conn.WriteMessages(
        kafka.Message{Value: []byte("one!")},
        kafka.Message{Value: []byte("two!")},
        kafka.Message{Value: []byte("three!")},
    )
    if err != nil {
        log.Fatal("failed to write messages:", err)
    }

    if err := conn.Close(); err != nil {
        log.Fatal("failed to close writer:", err)
    }
}
