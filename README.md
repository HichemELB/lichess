# Lichess Data Mesh

This repository contains the code and configuration files for the Lichess data mesh architecture. The architecture consists of multiple domain data products owned and managed by different teams. 

## Core Data

The `core-data` directory contains the shared code and configuration files that are used across all domain data products.

## Game Data

The `game-data` directory contains the code and configuration files for the game data domain data product. This product is owned and managed by the game mechanics team and includes data related to game play such as move data and match data. 

## User Experience

The `user-experience` directory contains the code and configuration files for the user experience domain data product. This product is owned and managed by the user experience team and includes data related to user behavior and preferences.

## Artificial Intelligence

The `artificial-intelligence` directory contains the code and configuration files for the artificial intelligence domain data product. This product is owned and managed by the artificial intelligence team and includes data related to game analysis.

## Adding a Domain Data Product

If a subset of data within a domain data product is identified as needing to be its own product, a new subdirectory can be created within the corresponding domain data product directory. The new product can then be managed by a new or existing team as appropriate.
