package main

import (
    "context"
    "fmt"
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
        log.Print("consumer sleeping for ", i, " seconds")
        time.Sleep(1 * time.Second)
    }
    log.Print("consumer started with: ", kafkaURL, topic)
    partition := 0
    conn, err := kafka.DialLeader(context.Background(), "tcp", kafkaURL, topic, partition)
    if err != nil {
        log.Fatal("failed to dial leader:", err)
    }

    conn.SetReadDeadline(time.Now().Add(10*time.Second))
    batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

    b := make([]byte, 10e3) // 10KB max per message
    for {
        n, err := batch.Read(b)
        if err != nil {
            break
        }
        fmt.Println(string(b[:n]))
    }

    if err := batch.Close(); err != nil {
        log.Fatal("failed to close batch:", err)
    }

    if err := conn.Close(); err != nil {
        log.Fatal("failed to close connection:", err)
    }
}
