# Transit Sims

## Overview

Transit Sims is a browser-based public transport simulation designed to demonstrate and test `doer-lib`, a Jakarta EE library for annotation processing and code generation.

The backend manages the transport network, buses, and simulation time. Passengers are JavaScript/TypeScript actors running in the browser. The frontend visualizes the city and animates buses and passengers using SVG or D3.

## Transport Network

Each simulation is created from a JSON configuration containing:

- **Road graph:** vertices with coordinates and bidirectional edges. The backend calculates edge lengths from vertex coordinates.
- **Stops:** identifiers, names, and coordinates. The backend connects each stop to its nearest graph vertex.
- **Routes:** identifiers, names, and ordered lists of stops.
- **Bus count:** the total number of buses. The backend distributes buses among routes proportionally to route length.

The backend calculates shortest paths between consecutive stops using the road graph. Buses travel along these paths, with movement determined by departure time and edge traversal durations.

When a bus reaches a terminal stop, it reverses direction and follows its route in reverse.

## Passengers

Each passenger is a browser-side actor with an origin and a destination. Before the simulation starts, the actor plans a journey consisting of one or more transit legs, including transfers between routes.

A passenger:
1. Walks from their origin to the boarding stop.
2. Waits for a suitable bus.
3. Boards only while the bus is stopped at that stop.
4. May transfer between routes at stops.
5. Alights only while the bus is stopped at the relevant stop.
6. Walks from the final stop to their destination.

The backend is authoritative for bus occupancy and boarding/alighting operations. Concurrent passenger requests must be handled consistently, respecting bus capacity and movement state.

## Backend API

The REST API supports:
- Creating a transit simulation from JSON.
- Starting, pausing, and resuming a simulation.
- Retrieving simulation status, network data, and bus states.
- Requesting passenger boarding and alighting.

An SSE endpoint streams events such as `BusDeparted`, `BusArrived`, `PassengerBoarded`, `PassengerAlighted`, and `SimulationCompleted`.

## Frontend

The browser renders the static road network, stops, and routes, and animates buses and passengers. Bus positions can be interpolated from the calculated path and timing information. Passenger actors communicate with the backend through HTTP and react to confirmed state changes delivered through SSE.

## Simulation Lifecycle

The simulation completes when all passengers have reached their destinations and all buses are at terminal stops. The backend owns simulation time and determines when this condition is satisfied.

## Primary Goal

Keep the implementation small but architecturally meaningful. Use Transit Sims to exercise `doer-lib` features such as actor messaging, asynchronous request handling, concurrency, timers, state transitions, and communication between browser-side and backend actors.