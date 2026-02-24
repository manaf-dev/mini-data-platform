"""Healthcare synthetic data generator with intentional data quality issues."""

import logging
from datetime import timedelta
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd
import pendulum
from faker import Faker

from .config import CITIES, DEPARTMENTS, ROOM_TYPES, TREATMENTS, GeneratorConfig

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class HealthcareDataGenerator:
    """Generate synthetic healthcare operational data with realistic quality issues."""

    def __init__(self, config: GeneratorConfig):
        self.config = config
        self.fake = Faker()
        Faker.seed(config.random_seed)
        np.random.seed(config.random_seed)

        self.output_path = Path(config.output_dir)
        self.output_path.mkdir(parents=True, exist_ok=True)

        # Store generated data for relationships
        self.patients_df = None
        self.visits_df = None
        self.admissions_df = None
        self.treatments_df = None
        self.billing_df = None

    def generate_all(self) -> Dict[str, pd.DataFrame]:
        """Generate all datasets in proper order."""
        logger.info("Starting healthcare data generation")

        datasets = {}

        logger.info(f"Generating {self.config.num_patients} patients")
        self.patients_df = self.generate_patients()
        datasets["patients"] = self.patients_df

        logger.info(f"Generating {self.config.num_visits} visits")
        self.visits_df = self.generate_visits()
        datasets["visits"] = self.visits_df

        logger.info("Generating admissions")
        self.admissions_df = self.generate_admissions()
        datasets["admissions"] = self.admissions_df

        logger.info(f"Generating {self.config.num_treatments} treatments")
        self.treatments_df = self.generate_treatments()
        datasets["treatments"] = self.treatments_df

        logger.info("Generating billing records")
        self.billing_df = self.generate_billing()
        datasets["billing"] = self.billing_df

        # Inject data quality issues
        logger.info("Injecting data quality issues for validation testing")
        datasets = self._inject_data_quality_issues(datasets)

        logger.info("Data generation complete")
        return datasets

    def generate_patients(self) -> pd.DataFrame:
        """Generate patient dimension data."""
        patients = []

        for i in range(1, self.config.num_patients + 1):
            # Registration date weighted toward recent years
            days_range = (self.config.end_date - self.config.start_date).days
            registration_offset = int(np.random.beta(2, 5) * days_range)
            registration_date = self.config.start_date + timedelta(
                days=registration_offset
            )

            # Birth date ensuring patients are between 0-90 years old
            age_years = np.random.gamma(shape=3, scale=15)
            age_years = min(90, max(0, age_years))
            birth_date = registration_date - timedelta(days=int(age_years * 365.25))

            patients.append(
                {
                    "patient_id": f"P{i:06d}",
                    "gender": np.random.choice(["M", "F"], p=[0.48, 0.52]),
                    "birth_date": birth_date.date(),
                    "city": np.random.choice(CITIES),
                    "registration_date": registration_date.date(),
                }
            )

        return pd.DataFrame(patients)

    def generate_visits(self) -> pd.DataFrame:
        """Generate visit fact data."""
        if self.patients_df is None:
            raise ValueError("Must generate patients first")

        visits = []
        patient_ids = self.patients_df["patient_id"].tolist()

        # Generate doctor IDs
        num_doctors = 50
        doctor_ids = [f"D{i:04d}" for i in range(1, num_doctors + 1)]

        for i in range(1, self.config.num_visits + 1):
            # Random patient, weighted toward some patients visiting more
            patient_id = np.random.choice(patient_ids)
            patient_reg_date = self.patients_df[
                self.patients_df["patient_id"] == patient_id
            ]["registration_date"].iloc[0]

            # Visit must be after patient registration
            visit_start = self._random_datetime_after(patient_reg_date)

            # Visit type
            visit_type = np.random.choice(
                ["Outpatient", "Emergency", "Inpatient"], p=[0.50, 0.30, 0.20]
            )

            # Visit duration based on type
            if visit_type == "Outpatient":
                duration_hours = np.random.uniform(0.5, 3)
            elif visit_type == "Emergency":
                duration_hours = np.random.uniform(1, 8)
            else:  # Inpatient
                duration_hours = np.random.uniform(4, 24)

            visit_end = visit_start + timedelta(hours=duration_hours)

            # Status
            status = np.random.choice(
                ["Completed", "Cancelled", "No-show"], p=[0.90, 0.05, 0.05]
            )

            visits.append(
                {
                    "visit_id": f"V{i:08d}",
                    "patient_id": patient_id,
                    "visit_type": visit_type,
                    "department": np.random.choice(DEPARTMENTS),
                    "doctor_id": np.random.choice(doctor_ids),
                    "visit_start_ts": visit_start,
                    "visit_end_ts": visit_end if status == "Completed" else None,
                    "status": status,
                }
            )

        return pd.DataFrame(visits)

    def generate_admissions(self) -> pd.DataFrame:
        """Generate admission records for inpatient visits."""
        if self.visits_df is None:
            raise ValueError("Must generate visits first")

        # Only inpatient visits can have admissions
        inpatient_visits = self.visits_df[
            (self.visits_df["visit_type"] == "Inpatient")
            & (self.visits_df["status"] == "Completed")
        ].copy()

        admissions = []

        for idx, visit in inpatient_visits.iterrows():
            # Length of stay (days)
            los_days = np.random.gamma(shape=2, scale=2)
            los_days = max(1, min(30, los_days))  # 1-30 days

            admit_ts = visit["visit_start_ts"]
            discharge_ts = admit_ts + timedelta(days=los_days)

            # Room type weighted toward general ward
            room_type = np.random.choice(
                [rt[0] for rt in ROOM_TYPES], p=[0.50, 0.25, 0.15, 0.07, 0.03]
            )

            admissions.append(
                {
                    "admission_id": f"A{len(admissions) + 1:08d}",
                    "visit_id": visit["visit_id"],
                    "admit_ts": admit_ts,
                    "discharge_ts": discharge_ts,
                    "room_type": room_type,
                }
            )

        logger.info(
            f"Generated {len(admissions)} admissions from {len(inpatient_visits)} inpatient visits"
        )
        return pd.DataFrame(admissions)

    def generate_treatments(self) -> pd.DataFrame:
        """Generate treatment records."""
        if self.visits_df is None:
            raise ValueError("Must generate visits first")

        completed_visits = self.visits_df[
            self.visits_df["status"] == "Completed"
        ].copy()

        treatments = []

        for idx, visit in completed_visits.iterrows():
            # Number of treatments per visit (1-5, weighted toward fewer)
            num_treatments = np.random.choice(
                [1, 2, 3, 4, 5], p=[0.40, 0.30, 0.15, 0.10, 0.05]
            )

            visit_start = visit["visit_start_ts"]
            visit_end = visit["visit_end_ts"]

            for _ in range(num_treatments):
                treatment_code, treatment_name, treatment_cost = TREATMENTS[
                    np.random.randint(0, len(TREATMENTS))
                ]

                # Treatment performed during visit
                if pd.notna(visit_end):
                    time_range = (visit_end - visit_start).total_seconds()
                    offset_seconds = np.random.uniform(0, time_range)
                    performed_ts = visit_start + timedelta(seconds=offset_seconds)
                else:
                    performed_ts = visit_start

                treatments.append(
                    {
                        "treatment_id": f"T{len(treatments) + 1:08d}",
                        "visit_id": visit["visit_id"],
                        "treatment_code": treatment_code,
                        "treatment_name": treatment_name,
                        "treatment_cost": treatment_cost,
                        "performed_ts": performed_ts,
                    }
                )

        return pd.DataFrame(treatments)

    def generate_billing(self) -> pd.DataFrame:
        """Generate billing records."""
        if self.visits_df is None or self.treatments_df is None:
            raise ValueError("Must generate visits and treatments first")

        completed_visits = self.visits_df[
            self.visits_df["status"] == "Completed"
        ].copy()

        billing = []

        for idx, visit in completed_visits.iterrows():
            # Calculate total from treatments
            visit_treatments = self.treatments_df[
                self.treatments_df["visit_id"] == visit["visit_id"]
            ]
            treatment_total = visit_treatments["treatment_cost"].sum()

            # Add room charges if admitted
            room_charges = 0
            if self.admissions_df is not None and len(self.admissions_df) > 0:
                admission = self.admissions_df[
                    self.admissions_df["visit_id"] == visit["visit_id"]
                ]
                if not admission.empty:
                    room_type = admission.iloc[0]["room_type"]
                    room_cost = next(rt[1] for rt in ROOM_TYPES if rt[0] == room_type)
                    los_days = (
                        admission.iloc[0]["discharge_ts"]
                        - admission.iloc[0]["admit_ts"]
                    ).days
                    room_charges = room_cost * max(1, los_days)

            total_amount = treatment_total + room_charges

            # Insurance coverage (60-80% for most, 0% for some)
            has_insurance = np.random.random() > 0.15
            if has_insurance:
                insurance_coverage = np.random.uniform(0.60, 0.80)
            else:
                insurance_coverage = 0

            insurance_amount = total_amount * insurance_coverage
            patient_amount = total_amount - insurance_amount

            # Payment status
            payment_status = np.random.choice(
                ["Paid", "Pending", "Partial"], p=[0.75, 0.15, 0.10]
            )

            # Payment timestamp
            if payment_status == "Paid":
                days_after_visit = np.random.gamma(shape=2, scale=5)
                payment_ts = visit["visit_end_ts"] + timedelta(days=days_after_visit)
            else:
                payment_ts = None

            billing.append(
                {
                    "bill_id": f"B{len(billing) + 1:08d}",
                    "visit_id": visit["visit_id"],
                    "total_amount": round(total_amount, 2),
                    "insurance_amount": round(insurance_amount, 2),
                    "patient_amount": round(patient_amount, 2),
                    "payment_status": payment_status,
                    "payment_ts": payment_ts,
                }
            )

        return pd.DataFrame(billing)

    def _inject_data_quality_issues(
        self, datasets: Dict[str, pd.DataFrame]
    ) -> Dict[str, pd.DataFrame]:
        """
        Inject realistic data quality issues that validation should catch.

        Issues injected:
        - Missing values in required fields
        - Duplicate records
        - Invalid foreign keys
        - Negative amounts
        - Future dates
        - Invalid enum values
        - Logical inconsistencies (end before start)
        """
        logger.info("Injecting data quality issues:")

        # PATIENTS: Missing gender, invalid city, duplicate IDs
        patients = datasets["patients"].copy()
        issue_count = int(len(patients) * 0.02)  # 2% issues

        # Missing gender
        missing_indices = np.random.choice(
            patients.index, size=issue_count // 3, replace=False
        )
        patients.loc[missing_indices, "gender"] = None
        logger.info(f"  - {len(missing_indices)} patients with missing gender")

        # Duplicate patient IDs
        dup_indices = np.random.choice(
            patients.index, size=issue_count // 3, replace=False
        )
        patients.loc[dup_indices, "patient_id"] = patients.loc[
            dup_indices[0], "patient_id"
        ]
        logger.info(f"  - {len(dup_indices)} duplicate patient IDs")

        datasets["patients"] = patients

        # VISITS: Missing required fields, invalid foreign keys, end before start
        visits = datasets["visits"].copy()
        issue_count = int(len(visits) * 0.03)  # 3% issues

        # Missing department
        missing_indices = np.random.choice(
            visits.index, size=issue_count // 4, replace=False
        )
        visits.loc[missing_indices, "department"] = None
        logger.info(f"  - {len(missing_indices)} visits with missing department")

        # Invalid patient_id (orphaned records)
        invalid_indices = np.random.choice(
            visits.index, size=issue_count // 4, replace=False
        )
        visits.loc[invalid_indices, "patient_id"] = "P999999"
        logger.info(f"  - {len(invalid_indices)} visits with invalid patient_id")

        # Visit end before start
        logic_indices = np.random.choice(
            visits[visits["visit_end_ts"].notna()].index,
            size=issue_count // 4,
            replace=False,
        )
        for idx in logic_indices:
            visits.loc[idx, "visit_end_ts"] = visits.loc[
                idx, "visit_start_ts"
            ] - timedelta(hours=2)
        logger.info(f"  - {len(logic_indices)} visits with end_ts before start_ts")

        # Invalid status
        invalid_indices = np.random.choice(
            visits.index, size=issue_count // 4, replace=False
        )
        visits.loc[invalid_indices, "status"] = "InvalidStatus"
        logger.info(f"  - {len(invalid_indices)} visits with invalid status")

        datasets["visits"] = visits

        # ADMISSIONS: Discharge before admit, invalid visit_id
        admissions = datasets["admissions"].copy()
        if len(admissions) > 0:
            issue_count = int(len(admissions) * 0.02)

            # Discharge before admit
            logic_indices = np.random.choice(
                admissions.index, size=issue_count // 2, replace=False
            )
            for idx in logic_indices:
                admissions.loc[idx, "discharge_ts"] = admissions.loc[
                    idx, "admit_ts"
                ] - timedelta(days=1)
            logger.info(
                f"  - {len(logic_indices)} admissions with discharge before admit"
            )

            # Invalid visit_id
            invalid_indices = np.random.choice(
                admissions.index, size=issue_count // 2, replace=False
            )
            admissions.loc[invalid_indices, "visit_id"] = "V99999999"
            logger.info(f"  - {len(invalid_indices)} admissions with invalid visit_id")

            datasets["admissions"] = admissions

        # TREATMENTS: Negative costs, missing treatment_code, invalid visit_id
        treatments = datasets["treatments"].copy()
        issue_count = int(len(treatments) * 0.025)

        # Negative costs
        negative_indices = np.random.choice(
            treatments.index, size=issue_count // 3, replace=False
        )
        treatments.loc[negative_indices, "treatment_cost"] = -abs(
            treatments.loc[negative_indices, "treatment_cost"]
        )
        logger.info(f"  - {len(negative_indices)} treatments with negative cost")

        # Missing treatment_code
        missing_indices = np.random.choice(
            treatments.index, size=issue_count // 3, replace=False
        )
        treatments.loc[missing_indices, "treatment_code"] = None
        logger.info(
            f"  - {len(missing_indices)} treatments with missing treatment_code"
        )

        # Invalid visit_id
        invalid_indices = np.random.choice(
            treatments.index, size=issue_count // 3, replace=False
        )
        treatments.loc[invalid_indices, "visit_id"] = "V99999999"
        logger.info(f"  - {len(invalid_indices)} treatments with invalid visit_id")

        datasets["treatments"] = treatments

        # BILLING: Negative amounts, insurance > total, missing visit_id
        billing = datasets["billing"].copy()
        issue_count = int(len(billing) * 0.02)

        # Negative total_amount
        negative_indices = np.random.choice(
            billing.index, size=issue_count // 3, replace=False
        )
        billing.loc[negative_indices, "total_amount"] = -abs(
            billing.loc[negative_indices, "total_amount"]
        )
        logger.info(f"  - {len(negative_indices)} bills with negative total_amount")

        # Insurance amount > total amount
        logic_indices = np.random.choice(
            billing.index, size=issue_count // 3, replace=False
        )
        billing.loc[logic_indices, "insurance_amount"] = (
            billing.loc[logic_indices, "total_amount"] * 1.5
        )
        logger.info(
            f"  - {len(logic_indices)} bills with insurance_amount > total_amount"
        )

        # Invalid visit_id
        invalid_indices = np.random.choice(
            billing.index, size=issue_count // 3, replace=False
        )
        billing.loc[invalid_indices, "visit_id"] = "V99999999"
        logger.info(f"  - {len(invalid_indices)} bills with invalid visit_id")

        datasets["billing"] = billing

        return datasets

    def save_to_csv(self, datasets: Dict[str, pd.DataFrame]) -> None:
        """Save all datasets to CSV files."""
        for name, df in datasets.items():
            filepath = self.output_path / f"{name}.csv"
            df.to_csv(filepath, index=False)
            logger.info(f"Saved {len(df)} records to {filepath}")

    def _random_datetime_after(self, start_date) -> pendulum.DateTime:
        """Generate random datetime between start_date and end_date."""
        start = pendulum.instance(pd.Timestamp(start_date))
        end = self.config.end_date

        days_range = (end - start).days
        if days_range <= 0:
            return start

        random_days = np.random.randint(0, days_range)
        random_date = start.add(days=random_days)

        # Add time component with business hours weighting
        hour = int(np.random.beta(2, 2) * 24)  # Weighted toward middle of day
        minute = np.random.randint(0, 60)

        return random_date.set(hour=hour, minute=minute, second=0)


def main():
    """Main execution function."""
    config = GeneratorConfig()
    generator = HealthcareDataGenerator(config)

    datasets = generator.generate_all()
    generator.save_to_csv(datasets)

    # Print summary statistics
    print("\n" + "=" * 60)
    print("HEALTHCARE DATA GENERATION SUMMARY")
    print("=" * 60)
    for name, df in datasets.items():
        print(f"{name:15s}: {len(df):,} records")
    print("=" * 60)
    print("\nData quality issues injected for validation testing:")
    print("  - Missing values in required fields")
    print("  - Duplicate records")
    print("  - Invalid foreign keys")
    print("  - Negative amounts")
    print("  - Logical inconsistencies (dates)")
    print("  - Invalid enum values")
    print("=" * 60)


if __name__ == "__main__":
    main()
