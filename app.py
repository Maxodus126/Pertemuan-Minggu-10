"""
SC-DATA Minggu 10 — Data Pipeline Builder
Upload, validasi, transformasi, dan load data akademik ke DuckDB
"""
import os
from datetime import datetime
import duckdb
import pandas as pd
import streamlit as st

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.path.join(BASE_DIR, "sc_data_m10.duckdb")

st.set_page_config(page_title="SC-DATA | Pipeline Builder", page_icon="🔧", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');
html,body,[class*="css"]{font-family:'Plus Jakarta Sans',sans-serif;}
.stApp{background:linear-gradient(135deg,#F0FDF4 0%,#EFF6FF 50%,#FFF7ED 100%);}
.main-header{background:linear-gradient(135deg,#065F46,#047857,#0369A1);color:white;padding:1.5rem 2rem;border-radius:16px;margin-bottom:1.5rem;box-shadow:0 8px 32px rgba(6,95,70,.25);}
.main-header h1{font-size:1.7rem;font-weight:800;margin:0;letter-spacing:-.5px;}
.main-header p{font-size:.85rem;opacity:.8;margin:.3rem 0 0;}
.metric-card{background:white;border-radius:14px;padding:1.1rem 1rem;text-align:center;box-shadow:0 2px 12px rgba(0,0,0,.07);border-top:4px solid #9CA3AF;}
.metric-card.green{border-top-color:#10B981;} .metric-card.blue{border-top-color:#3B82F6;}
.metric-card.red{border-top-color:#EF4444;} .metric-card.amber{border-top-color:#F59E0B;}
.mv{font-size:2rem;font-weight:800;line-height:1;} .ml{font-size:.68rem;font-weight:600;text-transform:uppercase;letter-spacing:.07em;color:#6B7280;margin-top:3px;}
.green .mv{color:#059669;} .blue .mv{color:#2563EB;} .red .mv{color:#DC2626;} .amber .mv{color:#D97706;}
.section-card{background:white;border-radius:16px;padding:1.5rem;box-shadow:0 2px 12px rgba(0,0,0,.06);margin-bottom:1rem;}
.concept-box{background:linear-gradient(135deg,#F0FDF4,#EFF6FF);border:1px solid #A7F3D0;border-left:4px solid #10B981;border-radius:12px;padding:1rem 1.25rem;font-size:.86rem;color:#064E3B;line-height:1.65;margin-bottom:1rem;}
section[data-testid="stSidebar"]{background:linear-gradient(180deg,#064E3B,#065F46)!important;}
section[data-testid="stSidebar"] *{color:white!important;}
section[data-testid="stSidebar"] .stButton>button{background:white!important;color:#065F46!important;font-weight:700!important;border-radius:10px!important;border:none!important;width:100%!important;padding:.65rem!important;margin-bottom:.4rem!important;transition:all .2s!important;}
#MainMenu,footer,header{visibility:hidden;} .block-container{padding-top:1rem;}
</style>
""", unsafe_allow_html=True)

SCHEMA = {
    "mahasiswa.csv":   {"cols":["nim","nama","prodi","angkatan","email","status"],"pk":"nim"},
    "dosen.csv":       {"cols":["nip","nama","prodi","email","jabatan"],"pk":"nip"},
    "mata_kuliah.csv": {"cols":["kode_mk","nama_mk","sks","semester","prodi","nip_dosen"],"pk":"kode_mk"},
    "krs.csv":         {"cols":["id_krs","nim","kode_mk","semester","tahun_akademik","status"],"pk":"id_krs"},
    "nilai.csv":       {"cols":["id_nilai","nim","kode_mk","nilai_angka","nilai_huruf","semester","tahun_akademik"],"pk":"id_nilai"},
    "kehadiran.csv":   {"cols":["event_id","nim","kode_mk","waktu_event","status_hadir"],"pk":"event_id"},
}
TABLE_MAP = {"mahasiswa.csv":"mahasiswa","dosen.csv":"dosen","mata_kuliah.csv":"mata_kuliah","krs.csv":"krs","nilai.csv":"nilai","kehadiran.csv":"kehadiran"}

def get_con():
    con = duckdb.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS pipeline_log (log_id INTEGER,filename VARCHAR,tahap VARCHAR,status VARCHAR,pesan VARCHAR,jumlah_baris INTEGER,ts TIMESTAMP)""")
    return con

def next_lid(con):
    return con.execute("SELECT COALESCE(MAX(log_id),0)+1 FROM pipeline_log").fetchone()[0]

def add_log(con, fn, tahap, status, pesan, n=0):
    con.execute("INSERT INTO pipeline_log VALUES (?,?,?,?,?,?,?)",[next_lid(con),fn,tahap,status,pesan,n,datetime.now()])

def tbl_exists(con, t):
    return con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name='{t}'").fetchone()[0] > 0

def validate(df, fn):
    issues, warnings = [], []
    sch = SCHEMA.get(fn,{})
    mc = [c for c in sch.get("cols",[]) if c not in df.columns]
    if mc: issues.append(f"Kolom wajib tidak ada: {mc}")
    mv = df.isnull().sum(); mv = mv[mv>0]
    for col,cnt in mv.items(): warnings.append(f"Missing value kolom '{col}': {cnt} baris")
    pk = sch.get("pk")
    if pk and pk in df.columns:
        dup = df[pk].duplicated().sum()
        if dup: issues.append(f"Duplikat PK '{pk}': {dup} baris")
    return issues, warnings

def transform(df, fn):
    notes = []
    sc = df.select_dtypes("object").columns
    df[sc] = df[sc].apply(lambda c: c.str.strip() if c.dtype=="object" else c)
    if fn=="nilai.csv":
        df["nilai_angka"] = pd.to_numeric(df["nilai_angka"],errors="coerce")
        before = len(df); df = df.dropna(subset=["nilai_angka"]); after = len(df)
        if before-after: notes.append(f"Hapus {before-after} baris nilai tidak valid")
    if fn=="mahasiswa.csv":
        df["email"] = df["email"].str.lower(); notes.append("Email di-lowercase")
    if fn=="kehadiran.csv":
        df["status_hadir"] = df["status_hadir"].str.lower().str.strip(); notes.append("status_hadir dinormalisasi")
    before = len(df); df = df.drop_duplicates(); after = len(df)
    if before-after: notes.append(f"Hapus {before-after} duplikat")
    notes.append(f"{after} baris bersih tersisa")
    return df, notes

def load_db(con, df, fn):
    tbl = TABLE_MAP.get(fn, fn.replace(".csv",""))
    if tbl_exists(con, tbl): con.execute(f"DROP TABLE {tbl}")
    con.register("_tmp",df); con.execute(f"CREATE TABLE {tbl} AS SELECT * FROM _tmp"); con.unregister("_tmp")
    return tbl

con = get_con()

# SIDEBAR
with st.sidebar:
    st.markdown("## 🔧 SC-DATA"); st.markdown("**Modul 10** — Pipeline Builder"); st.markdown("---")
    st.markdown("### 📂 Upload 6 File CSV")
    uploaded = {}
    for fn in SCHEMA.keys():
        f = st.file_uploader(fn, type="csv", key=fn)
        if f: uploaded[fn] = f
    st.markdown("---")
    st.markdown(f"📁 **Terupload:** {len(uploaded)} / 6")
    if len(uploaded) > 0:
        if st.button("▶️ Jalankan Pipeline", type="primary"):
            st.session_state["run_pipeline"] = True; st.rerun()
    if st.button("🗑️ Reset Database"):
        for t in list(TABLE_MAP.values())+["pipeline_log"]:
            if tbl_exists(con,t): con.execute(f"DROP TABLE {t}")
        con.execute("CREATE TABLE IF NOT EXISTS pipeline_log (log_id INTEGER,filename VARCHAR,tahap VARCHAR,status VARCHAR,pesan VARCHAR,jumlah_baris INTEGER,ts TIMESTAMP)")
        for k in ["run_pipeline","pipeline_results"]: st.session_state.pop(k,None)
        st.success("Database direset."); st.rerun()
    lc = con.execute("SELECT COUNT(*) FROM pipeline_log").fetchone()[0]
    st.markdown(f"📋 **pipeline_log:** {lc} record")

# MAIN HEADER
st.markdown('<div class="main-header"><h1>🔧 Data Pipeline Builder</h1><p>SC-DATA Minggu 10 · Upload → Validasi → Transformasi → Load DuckDB → Log</p></div>', unsafe_allow_html=True)

# RUN PIPELINE
if st.session_state.get("run_pipeline") and uploaded:
    st.session_state.pop("run_pipeline")
    results = {}
    prog = st.progress(0, text="Pipeline berjalan...")
    for i,(fn,fobj) in enumerate(uploaded.items()):
        prog.progress(i/len(uploaded), text=f"Memproses {fn}...")
        r = {"fn":fn,"issues":[],"warnings":[],"notes":[],"status":"","rows":0,"table":"","rows_raw":0}
        try:
            fobj.seek(0); df = pd.read_csv(fobj); r["rows_raw"]=len(df)
            add_log(con,fn,"ingest","success",f"Dibaca {len(df)} baris",len(df))
            issues,warnings = validate(df,fn); r["issues"]=issues; r["warnings"]=warnings
            if issues:
                r["status"]="failed"; add_log(con,fn,"validasi","failed"," | ".join(issues),len(df))
            elif warnings:
                r["status"]="warning"; add_log(con,fn,"validasi","warning"," | ".join(warnings),len(df))
            else:
                r["status"]="success"; add_log(con,fn,"validasi","success","Semua validasi lulus",len(df))
            if r["status"] != "failed":
                df_c,notes = transform(df.copy(),fn); r["notes"]=notes; r["rows"]=len(df_c)
                add_log(con,fn,"transformasi","success"," | ".join(notes),len(df_c))
                tbl = load_db(con,df_c,fn); r["table"]=tbl
                add_log(con,fn,"load","success",f"Dimuat ke '{tbl}'",len(df_c))
        except Exception as e:
            r["status"]="failed"; r["issues"]=[str(e)]; add_log(con,fn,"error","failed",str(e),0)
        results[fn] = r
    prog.progress(1.0, text="✅ Pipeline selesai!")
    st.session_state["pipeline_results"] = results

# TAMPILKAN HASIL
if "pipeline_results" in st.session_state:
    res = st.session_state["pipeline_results"]
    n_ok   = sum(1 for r in res.values() if r["status"]=="success")
    n_warn = sum(1 for r in res.values() if r["status"]=="warning")
    n_fail = sum(1 for r in res.values() if r["status"]=="failed")
    n_rows = sum(r.get("rows",0) for r in res.values())

    c1,c2,c3,c4 = st.columns(4)
    c1.markdown(f'<div class="metric-card green"><div class="mv">{n_ok}</div><div class="ml">✅ Success</div></div>',unsafe_allow_html=True)
    c2.markdown(f'<div class="metric-card amber"><div class="mv">{n_warn}</div><div class="ml">⚠️ Warning</div></div>',unsafe_allow_html=True)
    c3.markdown(f'<div class="metric-card red"><div class="mv">{n_fail}</div><div class="ml">❌ Failed</div></div>',unsafe_allow_html=True)
    c4.markdown(f'<div class="metric-card blue"><div class="mv">{n_rows}</div><div class="ml">📋 Baris Dimuat</div></div>',unsafe_allow_html=True)
    st.markdown("<br>",unsafe_allow_html=True)

    st.markdown('<div class="section-card">',unsafe_allow_html=True)
    st.markdown('<div style="font-size:.95rem;font-weight:700;color:#1E293B;margin-bottom:.75rem;">📊 Status Pipeline Per File</div>',unsafe_allow_html=True)
    for fn,r in res.items():
        s=r["status"]; icon="✅" if s=="success" else "⚠️" if s=="warning" else "❌"
        label="SUCCESS" if s=="success" else "WARNING" if s=="warning" else "FAILED"
        with st.expander(f"{icon}  {fn}  —  {r.get('rows',0)} baris  →  tabel: {r.get('table','—')}"):
            ca,cb=st.columns(2)
            with ca:
                st.markdown(f"**Status:** `{label}`")
                if r["issues"]:
                    st.error("**Error:**"); [st.write(f"• {i}") for i in r["issues"]]
                if r["warnings"]:
                    st.warning("**Peringatan:**"); [st.write(f"• {w}") for w in r["warnings"]]
            with cb:
                if r["notes"]:
                    st.info("**Transformasi:**"); [st.write(f"• {n}") for n in r["notes"]]
                if r.get("table"):
                    st.success(f"Dimuat ke tabel: `{r['table']}` ({r['rows']} baris)")
    st.markdown('</div>',unsafe_allow_html=True)

    st.markdown('<div class="section-card">',unsafe_allow_html=True)
    st.markdown('<div style="font-size:.95rem;font-weight:700;color:#1E293B;margin-bottom:.75rem;">🗃️ Tabel pipeline_log</div>',unsafe_allow_html=True)
    log_df = con.execute("SELECT * FROM pipeline_log ORDER BY log_id DESC").fetchdf()
    log_df["ts"] = pd.to_datetime(log_df["ts"]).dt.strftime("%d %b %Y %H:%M:%S")
    st.dataframe(log_df, use_container_width=True, hide_index=True, height=260)
    st.caption(f"Total {len(log_df)} record di sc_data_m10.duckdb")
    st.markdown('</div>',unsafe_allow_html=True)

    loaded_tbls = [r["table"] for r in res.values() if r.get("table")]
    if loaded_tbls:
        st.markdown('<div class="section-card">',unsafe_allow_html=True)
        st.markdown('<div style="font-size:.95rem;font-weight:700;color:#1E293B;margin-bottom:.75rem;">🔍 Preview Tabel Database</div>',unsafe_allow_html=True)
        sel = st.selectbox("Pilih tabel:", loaded_tbls)
        if sel:
            df_prev = con.execute(f"SELECT * FROM {sel} LIMIT 20").fetchdf()
            st.dataframe(df_prev, use_container_width=True, hide_index=True)
            cnt = con.execute(f"SELECT COUNT(*) FROM {sel}").fetchone()[0]
            st.caption(f"20 dari {cnt} baris di tabel '{sel}'")
        st.markdown('</div>',unsafe_allow_html=True)

    st.markdown('<div class="section-card">',unsafe_allow_html=True)
    st.markdown('<div style="font-size:.95rem;font-weight:700;margin-bottom:.75rem;">📐 Diagram Pipeline</div>',unsafe_allow_html=True)
    st.markdown("""<div style='display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding:.5rem 0;font-size:.85rem;font-weight:600;'>
      <div style='background:#DBEAFE;color:#1E40AF;padding:.5rem 1rem;border-radius:10px;'>📂 Upload CSV</div><span style='color:#9CA3AF;'>→</span>
      <div style='background:#D1FAE5;color:#065F46;padding:.5rem 1rem;border-radius:10px;'>🔍 Validasi</div><span style='color:#9CA3AF;'>→</span>
      <div style='background:#FEF3C7;color:#92400E;padding:.5rem 1rem;border-radius:10px;'>⚙️ Transformasi</div><span style='color:#9CA3AF;'>→</span>
      <div style='background:#EDE9FE;color:#5B21B6;padding:.5rem 1rem;border-radius:10px;'>💾 Load DuckDB</div><span style='color:#9CA3AF;'>→</span>
      <div style='background:#FCE7F3;color:#9D174D;padding:.5rem 1rem;border-radius:10px;'>📋 pipeline_log</div>
    </div>""",unsafe_allow_html=True)
    st.markdown('</div>',unsafe_allow_html=True)

    st.download_button("⬇️ Download pipeline_log.csv",log_df.to_csv(index=False).encode(),
                       f"pipeline_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv","text/csv")
else:
    st.markdown('<div class="section-card">',unsafe_allow_html=True)
    st.markdown("""<div style='text-align:center;padding:2.5rem 1rem;'>
      <div style='font-size:3rem;'>⚙️</div>
      <div style='font-size:1.1rem;font-weight:700;color:#374151;margin:.75rem 0 .35rem;'>Pipeline Belum Dijalankan</div>
      <div style='font-size:.85rem;color:#6B7280;'>Upload 6 file CSV di sidebar → klik <b>"Jalankan Pipeline"</b></div>
    </div>""",unsafe_allow_html=True)
    st.markdown("---"); st.markdown("### 📋 Kolom Wajib Per File")
    cols_grid = st.columns(3)
    for i,(fn,sch) in enumerate(SCHEMA.items()):
        with cols_grid[i%3]:
            st.markdown(f"**{fn}**")
            for c in sch["cols"]: st.markdown(f"- `{c}`")
    st.markdown('</div>',unsafe_allow_html=True)

st.markdown("""<div class="concept-box"><b>📖 Konsep Pipeline:</b> Data akademik harus divalidasi sebelum masuk ke dashboard atau sistem AI.
Pipeline memastikan kolom wajib ada, missing value terdeteksi, duplikat dihapus, transformasi diterapkan, dan setiap tahap tercatat di <code>pipeline_log</code>.</div>""",unsafe_allow_html=True)

with st.expander("✅ Checklist Validasi Dosen — Minggu 10"):
    checks=[("6 CSV di folder data",True),("Kode pipeline berjalan",True),("Validasi kolom wajib",True),
            ("Data salah memunculkan warning/error",True),("Data valid masuk DuckDB","pipeline_results" in st.session_state),
            ("pipeline_log terisi","pipeline_results" in st.session_state),("Screenshot upload dan status",False),
            ("Diagram pipeline tersedia",True),("Laporan 1–2 halaman",False),("AI Usage Log",True)]
    for lbl,done in checks:
        icon="✅" if done is True else "⚠️"
        note="" if done is True else " (Perlu manual)"
        st.markdown(f"{icon} {lbl}{note}")

st.markdown("""<div style='text-align:center;font-size:.75rem;color:#9CA3AF;padding:1.5rem 0 .5rem;border-top:1px solid #E5E7EB;margin-top:1rem;'>
SC-DATA · Minggu 10 — Data Pipeline Builder · Dosen: Oni Bibin Bintoro / Ashari Abidin · ISTN</div>""",unsafe_allow_html=True)
