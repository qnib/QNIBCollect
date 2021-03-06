package main

import (
    "log"
    "os"

    zmq "github.com/pebbe/zmq4"
)

func main() {
    // Create a dealer socket and connect it to the ZmqPUB socket.
    socket, err := zmq.NewSocket(zmq.SUB)
    if err != nil {
        log.Fatal("Socket-Err: ", err)
    }
    socket.SetSubscribe("")
	  socket.Connect(os.Args[1])
    //defer socket.Close()

    log.Println("Subscriber created and connected")

    // Receve the message. Here we call RecvMessage, which
    // will return the message as a slice of frames ([][]byte).
    // Since this is a router socket that support async
    // request / reply, the first frame of the message will
    // be the routing frame.
    for {
        msg, err := socket.Recv(0)
        if err != nil {
          log.Fatal("Rcv-Err: ", err)
        }

        log.Printf("Message '%s' received", msg)
    }

}
