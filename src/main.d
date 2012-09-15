
import core.thread;
import core.time;

import std.stdio;
import std.string;

import dzmq, devices;

void cmain() {
	Context context = new Context(1);
	
	// Socket to talk to server
	writef("Connecting to hello world server…\n");
	Socket requester = new Socket(context, Socket.Type.SUB);
	requester.connect("tcp://localhost:5667");
	requester.subscribe("ZMQTesting");
	auto rs = new SocketStream(requester);
	
	foreach(s;rs) {
		"Received: %s".writefln(s);
	}
}

void smain()
{
	Context context = new Context(1);
	
	// Socket to talk to clients
	Socket responder = new Socket(context, Socket.Type.PUB);
	responder.bind("tcp://*:5667");
	
	int i=0;
	while (1) {
		// Wait for next request from client
		responder.send_multipart(["ZMQTesting", "%d".format(i++)]);
		
		// Do some 'work'
		Thread.sleep(dur!"seconds"(1));
	}
}

void dmain()
{
	Context context = new Context(1);
	
	// Socket to talk to clients
	Socket front = new Socket(context, Socket.Type.SUB);
	front.connect("tcp://localhost:5668");
	front.subscribe("");
	
	Socket back = new Socket(context, Socket.Type.PUB);
	back.bind("tcp://*:5667");
	
	auto dev = new ForwarderDevice(front, back);
}

void main(string[] argv) {
	if(argv.length != 2) {
		stderr.writeln("Error: Invalid arguments");
		return;
	}
	if(argv[1] == "server") smain();
	else if(argv[1] == "client") cmain();
	else if(argv[1] == "device") dmain();
}
