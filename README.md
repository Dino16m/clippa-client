# Clippa Client

A Go-based client application for Clippa, a distributed clipboard sharing system that enables secure clipboard synchronization across multiple devices.

## Overview

Clippa Client is a component of the Clippa project that manages clipboard synchronization in a decentralized network. It handles clipboard monitoring, secure data transmission, and coordination with other Clippa parties (nodes) in the network.

## Features

- **Clipboard Monitoring**: Real-time detection and synchronization of clipboard changes
- **Secure Communication**: WebSocket-based communication with TLS encryption support
- **Party Management**: Integration with multiple Clippa parties for distributed clipboard sharing
- **Local & Global Management**: Support for both local server connections and global party management
- **Conclave Voting System**: Consensus-based leader election among party members
- **Error Handling**: Comprehensive error management and logging

## Architecture

The project is organized into the following components:

### Internal Packages

#### `clip/`

- **manager.go**: Manages clipboard read/write operations and synchronizes clipboard content across connected parties

#### `manager/`

- **client.go**: HTTP/WebSocket client for communicating with Clippa servers
- **local.go**: Handles communication with local Clippa servers
- **global.go**: Manages global party connections and coordination
- **conclave.go**: Implements consensus mechanisms for party decision-making
- **local_server.go**: Local server implementation for party communication
- **types.go**: Data structures for parties, ballots, clipboard data, and TLS configuration
- **utils.go**: Utility functions for various operations
- **auth.go**: Authentication and authorization handling

## Requirements

- Go 1.25.5 or later

## Dependencies

- `github.com/sirupsen/logrus`: Logging library
- `github.com/google/uuid`: UUID generation
- `golang.design/x/clipboard`: Cross-platform clipboard access
- `github.com/coder/websocket`: WebSocket support

## Installation

### Clone the Repository

```bash
git clone https://github.com/dino16m/clippa-client.git
cd clippa-client
```

### Install Dependencies

```bash
go mod download
```

## Building

```bash
go build -o clippa-client ./cmd
```

## Usage

Run the client:

```bash
./clippa-client
```

Configuration details and command-line flags should be documented in the main.go file or provided in the project's documentation.

## Data Types

### Party

Represents a node in the Clippa network with ID, name, leader address, and TLS certificates.

### ClipboardData

Container for clipboard content to be synchronized across parties.

### Ballot

Represents a vote during conclave elections, containing party address, reachability, and latency information.

### VoteData

Aggregates multiple ballots for consensus decisions.

## Security

- TLS encryption for secure communication between parties
- Secret-based authentication for party identification
- Certificate-based mutual authentication support

## License

[Add license information if available]

## Contributing

[Add contributing guidelines if available]

## Support

For issues, questions, or contributions, please visit the [Clippa project repository](https://github.com/dino16m/clippa-client).
