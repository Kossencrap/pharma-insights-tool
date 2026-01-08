import sqlite3
conn=sqlite3.connect(r"data/powershell-checks/europepmc.sqlite")
conn.execute("DELETE FROM sentence_events")
conn.commit()
conn.close()
