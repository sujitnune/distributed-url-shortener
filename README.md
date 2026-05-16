# Distributed URL Shortener with Replication

A fault-tolerant and scalable Distributed URL Shortener built using Python, Flask, PostgreSQL, Consistent Hashing, Replication, Load Balancing, and CDN Caching.

This project demonstrates core distributed systems concepts such as:
- Consistent Hashing
- Replication
- Round Robin Load Balancing
- CDN-style Edge Caching
- Fault Tolerance
- Cloud Deployment

## Features

- Generate deterministic 7-character short URLs
- Round-robin request distribution across backend servers
- Consistent hashing for storage node allocation
- Replication factor of 3 for high availability
- CDN cache with TTL support
- Real-time dashboard for monitoring
- Storage node failure simulation
- PostgreSQL persistence
- Cloud deployment on Render

## Tech Stack

- Python 3.11
- Flask
- PostgreSQL
- Gunicorn
- HTML/CSS/JavaScript
- Render Cloud Platform

## System Architecture

Client → CDN Cache → Load Balancer → Backend Servers → Consistent Hash Ring → Storage Nodes → PostgreSQL

## Project Structure

```bash
├── url_shortener.py
├── schema.sql
├── requirements.txt
├── templates/
│   └── index.html
├── static/
├── render.yaml
└── README.md
