#!/usr/bin/env python3
import sys, io

# Force UTF-8 encoding for stdin and stdout
sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

current_country = None
total_cases = 0
total_deaths = 0
population = 0

for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) != 4:
        continue
    country, cases, deaths, pop = parts
    cases = int(cases)
    deaths = int(deaths)
    pop = int(pop)

    if current_country == country:
        total_cases += cases
        total_deaths += deaths
    else:
        if current_country:
            cases_per_100k = total_cases / float(population) * 100000
            deaths_per_100k = total_deaths / float(population) * 100000
            print("{}\t{:.2f}\t{:.2f}".format(current_country, cases_per_100k, deaths_per_100k))
        current_country = country
        total_cases = cases
        total_deaths = deaths
        population = pop

# Print last country
if current_country:
    cases_per_100k = total_cases / float(population) * 100000
    deaths_per_100k = total_deaths / float(population) * 100000
    print("{}\t{:.2f}\t{:.2f}".format(current_country, cases_per_100k, deaths_per_100k))

