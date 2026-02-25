"""Configuration for healthcare data generation."""

import os
from dataclasses import dataclass
from datetime import datetime

import pendulum


@dataclass
class GeneratorConfig:
    """Configuration parameters for data generation."""

    # Volume settings
    num_patients: int = os.getenv("NUM_PATIENTS", 5000)
    num_visits: int = os.getenv("NUM_VISITS", 15000)
    num_admissions: int = os.getenv("NUM_ADMISSIONS", 3000)
    num_treatments: int = os.getenv("NUM_TREATMENTS", 25000)

    # Date range
    start_date: datetime = pendulum.parse("2023-01-01")
    end_date: datetime = pendulum.parse("2024-12-31")

    # Business rules
    admission_rate: float = 0.20  # 20% of visits result in admission
    emergency_rate: float = 0.30  # 30% are emergency visits
    payment_completion_rate: float = 0.85  # 85% of bills paid

    # Output settings
    output_dir: str = "data/raw"

    # Seed for reproducibility
    random_seed: int = 42


# Department configuration
DEPARTMENTS = [
    "Emergency",
    "Cardiology",
    "Orthopedics",
    "Pediatrics",
    "Neurology",
    "Oncology",
    "General Surgery",
    "Internal Medicine",
    "Obstetrics",
    "Radiology",
]

# Treatment codes and costs
TREATMENTS = [
    ("LAB001", "Complete Blood Count", 45.00),
    ("LAB002", "Lipid Panel", 65.00),
    ("LAB003", "Metabolic Panel", 55.00),
    ("IMG001", "X-Ray Chest", 150.00),
    ("IMG002", "CT Scan", 800.00),
    ("IMG003", "MRI", 1200.00),
    ("IMG004", "Ultrasound", 250.00),
    ("SURG001", "Appendectomy", 5000.00),
    ("SURG002", "Knee Replacement", 15000.00),
    ("SURG003", "Cardiac Bypass", 35000.00),
    ("MED001", "IV Fluids", 120.00),
    ("MED002", "Antibiotic Course", 200.00),
    ("MED003", "Pain Management", 80.00),
    ("CONS001", "Specialist Consultation", 180.00),
    ("CONS002", "Follow-up Visit", 90.00),
    ("THER001", "Physical Therapy Session", 110.00),
    ("THER002", "Occupational Therapy", 125.00),
    ("EMRG001", "Emergency Room Visit", 450.00),
    ("EMRG002", "Trauma Care", 2500.00),
]

# Room types and daily costs
ROOM_TYPES = [
    ("General Ward", 300.00),
    ("Semi-Private", 500.00),
    ("Private", 800.00),
    ("ICU", 2000.00),
    ("CCU", 2500.00),
]

# Cities for patient distribution
CITIES = [
    "Accra",
    "Kumasi",
    "Tamale",
    "Takoradi",
    "Cape Coast",
    "Sunyani",
    "Koforidua",
    "Ho",
    "Wa",
    "Bolgatanga",
]
