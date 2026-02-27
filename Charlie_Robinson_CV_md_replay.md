# Charlie Robinson
Bradwell, NR31 9GD  
07985 127695  
Robinsoncharlie55@gmail.com  
linkedin.com/in/charlie-robinson-4742b1229  
GitHub: https://github.com/GeoCodeCrafter

## Profile
Data and systems-focused engineer with production geophysical data interpretation experience and strong Python automation background. Building Rust-based market data infrastructure focused on deterministic replay, binary parsing, and reproducible analytics workflows.

## Technical Skills
- Languages: Rust, Python, PowerShell
- Data Systems: event normalization, append-only storage, indexed seek, CRC validation
- Protocols and APIs: ITCH-style binary parsing, PCAP ingestion, gRPC streaming, HTTP ingestion
- Tools: IHS Kingdom, Sonarwiz, Shearwater Reveal, ArcGIS, AutoCAD
- Testing: unit, integration, property-based fuzz testing
- Engineering: performance benchmarking, workflow automation, technical reporting

## Projects
### md-replay | Rust market data normalization and deterministic replay
GitHub: https://github.com/GeoCodeCrafter/md-replay

- Designed canonical event model for trades and quotes with strict global sequencing and deterministic ordering.
- Implemented four ingestion paths: three CSV schemas and mocked multicast UDP PCAP feed (ITCH-style binary messages).
- Built safe binary parser with packet-length validation and malformed-packet handling with packet/offset diagnostics.
- Built append-only event log format with per-record CRC32 framing and stride index for fast replay window seeks.
- Implemented deterministic replay engine with sequence-ordered emission, time scaling, max-speed mode, and step mode.
- Exposed replay via gRPC streaming service and added CLI clients for print, feature generation, determinism verify, and benchmark runs.
- Added reproducibility harness that reruns feature output and verifies byte-for-byte identical results across runs.
- Added GUI dashboard with replay playback controls, feature charts (mid/spread/imbalance/vol), signal markers, and parser-diff view.
- Benchmarked replay throughput and latency and added CI quality gates (`fmt`, `clippy -D warnings`, `test`).

## Professional Experience
### Marine Geophysicist | Peak Processing, Wroxham
August 2024 - Present
- Interpret marine geophysical survey datasets and support client-facing deliverables.
- Build Python automation scripts to streamline data processing workflows and reduce manual processing steps.
- Process survey/client data in IHS Kingdom and supporting interpretation toolchain.
- Produce technical reports documenting survey findings, methods, and analysis outputs.
- Identify and implement process improvements across data collection and processing workflows.

### Python Web Development (Independent)
Ongoing
- Build web applications and broaden software engineering depth beyond core domain tooling.
- Explore machine learning workflows for structured, data-driven decision support.

### FTMO Funded Day Trader (Independent)
Personal Project
- Passed funded-account evaluation with disciplined risk management and consistent execution.
- Applied quantitative reasoning, pattern recognition, and process discipline under live constraints.

## Education
### University of East Anglia, Norwich, UK
BSc (Hons) Geology with Geography, Upper Second Class (2:1)  
September 2021 - July 2024

### East Norfolk Sixth Form College, Great Yarmouth, UK
A-Levels: Biology (B), Chemistry (B), Geography (A)  
September 2019 - July 2021

### Lynn Grove Academy, Great Yarmouth, UK
GCSEs: 9 subjects including English, Mathematics, Sciences  
September 2014 - July 2019

## Key Strengths
- Technical data interpretation and analysis in complex survey environments
- Workflow automation and process optimization through scripting
- Clear technical communication and report writing
- Fast ramp-up in new technical domains and tooling
