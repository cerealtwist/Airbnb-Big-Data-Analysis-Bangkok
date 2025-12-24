import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Konfigurasi Tampilan Grafik Akademik
sns.set_theme(style="whitegrid")
plt.rcParams["figure.figsize"] = (12, 6)
plt.rcParams["axes.titlesize"] = 14
plt.rcParams["axes.labelsize"] = 12

def main():
    # 1. Setup Path
    # Mengambil lokasi absolut dari folder project untuk menghindari error path
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    parquet_path = os.path.join(base_dir, "output", "airbnb_bangkok_monthly")
    
    print(f"Membaca data dari: {parquet_path}")
    
    # 2. Load Data Parquet
    try:
        df = pd.read_parquet(parquet_path)
        print("Data berhasil dimuat.")
    except Exception as e:
        print(f"Gagal membaca data: {e}")
        return

    # Validasi Data
    if df.empty:
        print("Dataframe kosong. Pastikan ETL pipeline sudah dijalankan.")
        return

    # Membuat kolom tanggal untuk sumbu X (tanggal 1 setiap bulan)
    df["date"] = pd.to_datetime(df["year"].astype(str) + "-" + df["month"].astype(str) + "-01")

    # ========================================================
    # GRAFIK 1: Dinamika Pasar (Harga vs Okupansi)
    # ========================================================
    # Agregasi data per bulan untuk melihat tren kota secara keseluruhan
    monthly_stats = df.groupby("date")[["avg_base_price", "occupancy_rate"]].mean().reset_index()

    fig, ax1 = plt.subplots()

    # Sumbu Y Kiri: Harga Listing (Garis Merah)
    sns.lineplot(
        data=monthly_stats, 
        x="date", 
        y="avg_base_price", 
        color="#D62728", # Merah Bata
        marker="o", 
        ax=ax1, 
        label="Rata-rata Harga Listing"
    )
    ax1.set_ylabel("Harga Listing (THB)", color="#D62728", fontweight="bold")
    ax1.tick_params(axis='y', labelcolor="#D62728")
    ax1.set_xlabel("Periode Proyeksi (Bulan)")

    # Sumbu Y Kanan: Tingkat Okupansi (Garis Biru)
    ax2 = ax1.twinx()
    sns.lineplot(
        data=monthly_stats, 
        x="date", 
        y="occupancy_rate", 
        color="#1F77B4", # Biru Standar
        marker="s", 
        linestyle="--", 
        ax=ax2, 
        label="Tingkat Okupansi"
    )
    ax2.set_ylabel("Tingkat Okupansi (0-1)", color="#1F77B4", fontweight="bold")
    ax2.tick_params(axis='y', labelcolor="#1F77B4")
    ax2.set_ylim(0, 1) # Skala 0% sampai 100% untuk konteks yang jelas

    plt.title("Proyeksi Dinamika Pasar Airbnb Bangkok: Harga vs Permintaan (2025-2026)", fontweight="bold")
    plt.tight_layout()
    
    output_file_1 = "viz_1_market_dynamics.png"
    plt.savefig(output_file_1, dpi=300)
    print(f"Grafik disimpan: {output_file_1}")

    # ========================================================
    # GRAFIK 2: Analisis Wilayah Premium (Top 10 Termahal)
    # ========================================================
    # Mengambil 10 wilayah dengan rata-rata harga listing tertinggi
    top_expensive = df.groupby("neighbourhood")["avg_base_price"].mean().nlargest(10).reset_index()

    plt.figure()
    sns.barplot(
        data=top_expensive, 
        x="avg_base_price", 
        y="neighbourhood", 
        palette="Reds_r"
    )
    plt.title("10 Wilayah dengan Rata-rata Harga Listing Tertinggi", fontweight="bold")
    plt.xlabel("Rata-rata Harga Listing (THB)")
    plt.ylabel("Wilayah / Distrik")
    plt.tight_layout()
    
    output_file_2 = "viz_2_premium_locations.png"
    plt.savefig(output_file_2, dpi=300)
    print(f"Grafik disimpan: {output_file_2}")

    # ========================================================
    # GRAFIK 3: Korelasi Harga dan Okupansi (Scatter Plot)
    # ========================================================
    plt.figure()
    sns.scatterplot(
        data=df, 
        x="avg_base_price", 
        y="occupancy_rate", 
        alpha=0.6, 
        color="purple",
        edgecolor="w"
    )
    plt.title("Distribusi Pasar: Hubungan Harga Listing vs Tingkat Okupansi", fontweight="bold")
    plt.xlabel("Harga Listing (THB)")
    plt.ylabel("Tingkat Okupansi (Rata-rata)")
    plt.grid(True, linestyle="--", alpha=0.5)
    
    output_file_3 = "viz_3_scatter_correlation.png"
    plt.savefig(output_file_3, dpi=300)
    print(f"Grafik disimpan: {output_file_3}")

if __name__ == "__main__":
    main()
