package main

import (
    "log"
    "os"
    "fmt"

    zmq "github.com/pebbe/zmq4"
)

func main() {
    // Create a dealer socket and connect it to the ZmqPUB socket.
    socket, err := zmq.NewSocket(zmq.REQ)
    if err != nil {
        log.Fatal("Socket-Err: ", err)
    }
    socket.Connect(os.Args[1])
    defer socket.Close()

    log.Println("Subscriber created and connected")

    // send hello
	msg := fmt.Sprintf("Hello")
	fmt.Println("Sending ", msg)
	socket.Send(msg, 0)
	// Wait for replies:
    for {
        reply, _ := socket.Recv(0)
        if reply == "EOM" {
            break
        } else {
            fmt.Println("Received ", reply)
        }
    }
}
