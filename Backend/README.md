# Socket.IO Chat

A simple chat demo for Socket.IO

## How to use

```
npm i
npm start
```

And point your browser to `http://localhost:3000`. Optionally, specify
a port by supplying the `PORT` env variable.  
Set the Redis endpoint by specifying the `REDIS_URL` env variable (default: redis://localhost:6379).

## Features

- Multiple users can join a chat room by each entering a unique username
on website load.
- Users can type chat messages to the chat room.
- A notification is sent to all users when a user joins or leaves
the chatroom.
- Multiple instances can run using Socket.IO Redis Adapter and Websocket Transport to avoid sticky sessions
