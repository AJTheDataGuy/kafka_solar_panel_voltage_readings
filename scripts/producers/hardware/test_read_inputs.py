"""Tests for the read inputs module

Module is intended to be run from the command line
with the pytest module
"""
import pytest
from .read_inputs import setup_adc_get_channel, get_single_input_voltage


def test_adc_channel_setup():
    """Test to ensure the ADC / I2C is detected properly at address 0x48

    Test fails if not connected
    """
    try:
        channel = setup_adc_get_channel()
    except ValueError:
        pytest.fail("I2C not connected at 0x48")


def test_voltage_reading_ok():
    """Tests that the Raspberry Pi is able to retrieve voltage readings
    from the mini solar panel and that those values are valid (above 0)
    """
    test_adc_channel_setup()
    channel = setup_adc_get_channel()
    voltage = get_single_input_voltage(channel)
    assert isinstance(voltage, float) and voltage > 0
