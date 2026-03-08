# Kontrola změn souborů

Skript, jenž projde adresářovou strukturu a uloží její stav do SQLite databáze.  
Při opakovaném spuštění zjišťuje změny ve složkách a souborech:

- create – nový soubor nebo složka
- delete – soubor nebo složka byla smazána
- modify – změnil se obsah nebo metadata
- rename – soubor nebo složka byla přejmenována (detekované podle velikosti, času změny a typu položky)

1. Skript projde celý adresář pomocí `os.scandir`.
2. Vytvoří snapshot aktuálního stavu.
3. Načte předchozí stav z databáze.
4. Porovná oba stavy.
5. Aktualizuje databázi.

Databáze obsahuje poslední známý stav databáze, případně ji vytvoří, pokud neexistuje

Výstup např.:

2026-03-08 16:13:59,278 - __main__ - INFO - Scan finished, scan_id=4  <br>
2026-03-08 16:13:59,278 - __main__ - INFO - Scanned items: 48970 <br>
2026-03-08 16:13:59,278 - __main__ - INFO - Created: 0 <br>
2026-03-08 16:13:59,278 - __main__ - INFO - Modified: 0 <br>
2026-03-08 16:13:59,278 - __main__ - INFO - Renamed: 215 <br>
2026-03-08 16:13:59,278 - __main__ - INFO - Deleted: 57 <br>

Poznámky:
- chybí podrobné logování
- kontrola u rename nemusí být absolutně spolehlivá (při úpravě a přejmenování souboru se vyhodnotí jako delete a create, bylo by možné řešit přes hash)

Dependencies:
- script používá pouze standardní Python knihovny

Spuštění:
- python main.py

