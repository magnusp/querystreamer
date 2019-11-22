#!/usr/bin/env bash

time (curl -s http://localhost:8080/data & curl -s http://localhost:8080/data) >/dev/null