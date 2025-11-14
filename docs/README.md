# Parallel Global Disaster Analyzer

## Build (recommended: WSL Ubuntu)
```
g++ -O3 -std=c++17 -fopenmp src/parallel_disaster_plus.cpp -o disaster_plus
```

## Run
```
./disaster_plus data/events_sample.csv 4
```

Outputs are written to `output/`:
- summary.txt
- daily_counts.csv
- hotspots.csv
- top_magnitudes.csv
- heatmap.ppm
- hotspots.kml
