# Mock-Medicare-Data-Generators
Creates Fake Medicare Data for Testing and Developing

# CMS Medicare Mock Data Generator and Parser

## Overview

This project is a personal portfolio effort to demonstrate my ability to work with complex Medicare data formats used by the Centers for Medicare & Medicaid Services (CMS). It includes Python scripts that generate and parse mock versions of key CMS files, specifically the **Medicare Advantage Monthly Membership Report (MMR)** and the **Medicare Advantage Organization Report (MOR)**.

All data is entirely **synthetic** and was created for **educational and demonstration purposes only**. No real patient or provider data is used.

## Project Goals

- Emulate the structure and logic of CMS fixed-width text files (MMR and MOR).
- Convert fixed-width data into structured `.csv` files for easier analysis.
- Provide a realistic demonstration of healthcare data engineering skills.
- Showcase the ability to automate parsing, QA, and transformation of CMS data formats.

## File Structure





## CMS Files Covered

### 🧾 MMR – Medicare Advantage Monthly Membership Report

- Contains monthly enrollment, payment, and plan data.
- Fixed-width format requiring field-level precision.
- Used for financial reconciliation and member tracking.

### 📄 MOR – Medicare Advantage Organization Report

- Includes HCC codes, demographic data, and risk scores.
- Used in Risk Adjustment processing and payment calculations.

## Skills Demonstrated

- Python programming for data generation and ETL
- Working with **fixed-width file formats**
- Parsing and transforming data into CSV
- Simulating **HCC coding**, member demographics, and plan identifiers
- Preparing data for **analytics, QA, or risk adjustment workflows**

## Disclaimer

> This project is not affiliated with CMS or any healthcare organization. All files are generated for demonstration purposes and contain **no real patient information**.

## Future Work

- Add support for MAO-004 and other Medicare reports
- Integrate mock HCC coefficients and risk score calculations
- Visualize trends using Power BI or Tableau
- Package as a reproducible Databricks workspace or Jupyter notebook

## Contact

Created by **Jason Votaw**  
📧 votawanalytics@gmail.com  
🔗 [LinkedIn](https://www.linkedin.com/in/jasonvotaw) | [GitHub](https://github.com/YOURUSERNAME)

