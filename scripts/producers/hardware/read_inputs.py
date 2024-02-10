"""Module to read raw inputs and voltages from a mini solar panel
using an Analog to Digital Converter (ADC) and Raspberry PI

This script is intended to be run on a Raspberry PI.

Requires the following:
1. A raspberry PI (I use Raspberry PI 4 model B) with SDA and SDC pins configured
Note: needs a keyboard, mouse, and monitor to connect to.
Details for Raspberry PI: https://www.altronics.com.au/p/z6302g-raspberry-pi-4-model-b-board-4gb/

2. An ADS1115 ADC (nice and high resolution!)
Details for the ADS1115: https://www.altronics.com.au/p/z6221-analog-to-digital-ADS1115-16-bit-converter-module/

3. A solar panel connected on ADC channel A0. I use the
N0720 0.75W solar panel from Altronics.
Details for the N0720 solar panel: https://www.altronics.com.au/p/n0720-mini-0.75w-polycrystalline-solar-project-panel/

4. Bread board

5. Other electronics equipment: Wires, solder, soldering iron, voltmeter, etc.
"""

import board
import busio
import adafruit_ads1x15.ads1115 as ADS
from adafruit_ads1x15.analog_in import AnalogIn


def setup_adc_get_channel():
    """Sets up the ADC channel

    Returns the ADC channel
    """
    # Create I2C bus
    i2c = busio.I2C(board.SCL, board.SDA)

    # Create the ADC object
    ads = ADS.ADS1115(i2c)

    # Create an analog input channel for A0 (single-ended)
    channel = AnalogIn(ads, ADS.P0)
    return channel


def get_single_input_voltage(adc_channel):
    """Reads a single voltage input from the ADC

    Returns the voltage as a float
    """
    # Can get the raw value - I have commented out for just the voltage
    # raw_value = chan.value

    # Calculate voltage (assuming 3.3V reference)
    voltage = adc_channel.voltage

    # Optional statement to print the values
    # print(f"Raw Value: {raw_value}, Voltage: {voltage}V")
    return voltage
