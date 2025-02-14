import pandas as pd
import numpy as np
from scipy.signal import correlate
import matplotlib.pyplot as plt
from dagster import job, op

# 1. Load CO2 emissions and energy consumption data
def load_data(co2_path: str, energy_path: str):
    co2_df = pd.read_csv(co2_path, parse_dates=['Date'], index_col='Date')
    energy_df = pd.read_csv(energy_path, parse_dates=['Date'], index_col='Date')
    
    # Ensure both datasets align by date
    merged_df = pd.merge(co2_df, energy_df, on='Date', how='inner')
    return merged_df

# 2. Apply Empirical Orthogonal Function (EOF) Matching (for patterns in emissions)
def empirical_orthogonal_function_matching(df):
    # Here, we're simplifying by doing a Principal Component Analysis (PCA), which is related to EOF
    from sklearn.decomposition import PCA
    
    # Assuming 'co2' and 'energy' are the columns of interest
    pca = PCA(n_components=2)
    pca_result = pca.fit_transform(df[['CO2_Emissions', 'Energy_Consumption']])
    
    return pca_result

# 3. Compute Cross-Correlation Function (CCF)
def compute_cross_correlation(df):
    co2_values = df['CO2_Emissions'].values
    energy_values = df['Energy_Consumption'].values
    ccf = correlate(co2_values, energy_values, mode='full', method='auto')
    
    return ccf

# 4. Compute Power Cross-Correlation Function (PCF)
def compute_power_cross_correlation(df):
    co2_values = df['CO2_Emissions'].values
    energy_values = df['Energy_Consumption'].values
    
    # Compute power cross-correlation (simplified)
    pcf = correlate(co2_values**2, energy_values**2, mode='full', method='auto')
    
    return pcf

# 5. Plot the results
def plot_results(ccf, pcf):
    # Plot Cross-Correlation Function
    plt.figure(figsize=(10, 6))
    plt.subplot(1, 2, 1)
    plt.plot(ccf)
    plt.title("Cross-Correlation Function (CCF)")

    # Plot Power Cross-Correlation Function
    plt.subplot(1, 2, 2)
    plt.plot(pcf)
    plt.title("Power Cross-Correlation Function (PCF)")
    
    plt.tight_layout()
    plt.show()

# Dagster Operations
@op
def load_co2_energy_data(co2_path: str, energy_path: str):
    return load_data(co2_path, energy_path)

@op
def apply_eof_matching(df):
    return empirical_orthogonal_function_matching(df)

@op
def compute_ccf(df):
    return compute_cross_correlation(df)

@op
def compute_pcf(df):
    return compute_power_cross_correlation(df)

@op
def plot_analysis(ccf, pcf):
    plot_results(ccf, pcf)

# Dagster Job (Pipeline)
@job
def emissions_analysis_pipeline():
    # Assuming you have the paths to your CO2 emissions and energy data
    co2_path = "/path/to/co2_emissions.csv"
    energy_path = "/path/to/energy_consumption.csv"

    df = load_co2_energy_data(co2_path, energy_path)
    eof_result = apply_eof_matching(df)
    ccf = compute_ccf(df)
    pcf = compute_pcf(df)
    plot_analysis(ccf, pcf)
