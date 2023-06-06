#!/usr/bin/env python3

import json

from pathlib import Path
from time import sleep
from tqdm.auto import tqdm
from typing import Dict, List, Iterable
import itertools
import tempfile

### Data loading class ###

class DataLoader:
    """Loads cars and counts the number of cars per model."""

    def __init__(self, read_delay=0):
        self._read_delay = read_delay
        self.car_model_counts = {}

    def _count_car(self, car: Dict):
        self.car_model_counts[car['car_model']] = self.car_model_counts.get(car['car_model'], 0) + 1

    def read_csv(self, file: Path) -> Iterable[Dict]:
        with open(file, 'r') as f:
            header = next(f)
            header = header.strip().split(',')
            for line in tqdm(f, desc=f'Reading {file.name}', unit='lines', leave=False):
                line = line.strip().split(',')
                if not line:
                    continue
                car = dict(zip(header, line))
                self._count_car(car)
                sleep(self._read_delay)
                yield car

    def read_json(self, file: Path) -> Iterable[Dict]:
        with file.open('r') as f:
            for line in tqdm(f, desc=f'Reading {file.name}', unit='lines', leave=False):
                line = line.strip()
                if not line:
                    continue
                car = json.loads(line)
                self._count_car(car)
                sleep(self._read_delay)
                yield car

    def _extract_xml_value(self, line: str) -> str:
        return line.split('>')[1].split('<')[0]

    def read_xml(self, file: Path) -> Iterable[Dict]:
        with file.open('r') as f:
            car = {}
            for line in tqdm(f, desc=f'Reading {file.name}', unit='lines', leave=False):
                line = line.strip()
                if line.startswith('<car_model'):
                    car['car_model'] = self._extract_xml_value(line)
                elif line.startswith('<year'):
                    car['year_of_manufacture'] = self._extract_xml_value(line)
                elif line.startswith('<price'):
                    car['price'] = self._extract_xml_value(line)
                elif line.startswith('<fuel'):
                    car['fuel'] = self._extract_xml_value(line)
                elif line.startswith('</row'):
                    self._count_car(car)
                    sleep(self._read_delay)
                    yield car
                    car = {}


### Data processing ###


def format_car(car: Dict, header: List) -> str:
    """Format a car into a csv line."""
    price = float(car['price'])
    car['price'] = f'{price:.2f}'
    car['car_model'] = car['car_model'].title()
    return ",".join([str(car.get(h, '')) for h in header])

### Main ###

if __name__ == '__main__':

    # TODO: these should be command line arguments
    DATA_DIR = Path(__file__).parent / 'dealership_data'
    OUTPUT_FILE = Path(__file__).parent / 'cars.csv'
    DELAY = 0.05

    # collect all csv, json, and xml files in the data directory
    csv_files = DATA_DIR.glob('*.csv')
    json_files = DATA_DIR.glob('*.json')
    xml_files = DATA_DIR.glob('*.xml')

    loader = DataLoader(read_delay=DELAY)

    with tempfile.NamedTemporaryFile(mode='w+') as car_buffer, \
         open(OUTPUT_FILE, 'w') as output_file:

        # concatenate all cars into a single iterable
        cars = itertools.chain(
            *([loader.read_csv(f) for f in csv_files] +
              [loader.read_json(f) for f in json_files] +
              [loader.read_xml(f) for f in xml_files])
        )

        # write all cars to a temporary file
        for car in tqdm(cars, desc=f'Buffering cars to {car_buffer.name}', unit='cars', leave=True):
            json.dump(car, car_buffer)
            car_buffer.write('\n')

        car_buffer.seek(0)

        # write the header
        header = ['car_model', 'year_of_manufacture', 'price', 'fuel']
        output_file.write(",".join(header))
        output_file.write('\n')

        # filter the cars
        retained_cars = (
            json.loads(line) for line in tqdm(car_buffer, desc='Filtering cars', unit='cars', leave=True)
        )
        retained_cars = (
            c for c in retained_cars
            if loader.car_model_counts[c['car_model']] >= 3
        )

        # write the cars to the final output file
        for car in tqdm(retained_cars, desc='Writing cars', unit='cars', leave=True):
            output_file.write(format_car(car, header))
            output_file.write('\n')
            sleep(DELAY)