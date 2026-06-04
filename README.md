# SC-DATA Minggu 10 — Data Pipeline Builder

> Upload, validasi, transformasi, dan load data akademik  
> Sistem Basis Data Modern dan Arsitektur Data SIAP AI

---

## Isi Paket

```
sc_data_minggu10/
├── app.py                         ← Aplikasi Streamlit (Python)
├── dashboard.html                 ← Versi website HTML standalone
├── requirements.txt               ← Dependency Python
├── sc_data_m10.duckdb             ← Database (terbuat otomatis)
├── data/
│   ├── mahasiswa.csv
│   ├── dosen.csv
│   ├── mata_kuliah.csv
│   ├── krs.csv
│   ├── nilai.csv                  ← Ada 1 baris missing value (sengaja untuk uji validasi)
│   └── kehadiran.csv
├── diagrams/
│   └── diagram_pipeline.mmd
├── reports/
│   └── narasi_minggu10.md
├── outputs/
│   ├── screenshots/
│   └── checklist_minggu10.csv
└── AI_Usage_Log.csv
```

---

## Cara Menjalankan

### Opsi A — Streamlit
```bash
pip install -r requirements.txt
streamlit run app.py
```
Upload 6 file CSV dari folder `data/` → klik "Jalankan Pipeline"

### Opsi B — HTML Standalone
Buka `dashboard.html` langsung di browser.  
Upload file CSV dari folder `data/` → klik "Jalankan Pipeline".

---

## Fitur Pipeline

| Tahap | Deskripsi |
|---|---|
| Ingest | Baca CSV, hitung jumlah baris |
| Validasi | Cek kolom wajib, missing value, duplikat PK |
| Transformasi | Strip whitespace, lowercase email, normalisasi status |
| Load | Simpan ke tabel DuckDB |
| Log | Setiap tahap dicatat ke pipeline_log |

---

## Status Pipeline

| Status | Kondisi |
|---|---|
| ✅ SUCCESS | Semua validasi lulus, data dimuat |
| ⚠️ WARNING | Ada missing value tapi data tetap diproses |
| ❌ FAILED | Kolom wajib tidak ada / duplikat PK kritis |

---

## Catatan: nilai.csv berisi 1 baris missing value
Baris NL020 sengaja dibuat tanpa nilai_angka dan nilai_huruf untuk menguji pipeline warning.

---

*Dosen: Oni Bibin Bintoro / Ashari Abidin · ISTN*
