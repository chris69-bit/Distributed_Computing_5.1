#!/bin/sh
# usage: ./build_report.sh DESIGN_M5.md TeleRM_Milestone5_Report.pdf "TeleRM · Milestone 5 · transactions"
set -e
IN="$1"; OUT="$2"; FOOT="$3"
pandoc "$IN" -f markdown+pipe_tables -t html5 -s --metadata pagetitle="$FOOT" -c report.css -o /tmp/report_build.html
wkhtmltopdf --quiet --enable-local-file-access -s A4 -T 16mm -B 18mm -L 14mm -R 14mm \
  --footer-center "$FOOT · page [page] of [topage]" --footer-font-size 7 /tmp/report_build.html "$OUT"
pdfinfo "$OUT" | grep Pages
