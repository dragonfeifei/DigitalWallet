#!/usr/bin/env bash
python src/antifraoud.py \
  -b paymo_input/batch_payment.csv \
  -s paymo_input/stream_payment.csv \
  -o paymo_output/