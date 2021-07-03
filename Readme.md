# Projekt inżynierski
## Opis
Projekt składa się z dwóch głównych części:
- program `EtlProcess.py` służący do transformacji danych
- interkatywny pulpit nawigacyjny będący finalną częścią produktu

## Transformacja danych
Program napisany jest w języku Python w wersji 3.6.5 przy użyciu oprogramowania Apache Spark 
w wersji 2.4.7 oraz współpracującego z nim produktem Apache Hadoop wersja 2.7. Dodatkowo należy posiadać Jave 8. Wszytskie wymienione technologie należy zainstalować, aby prawidłoweo działała ta części projektu.

Moduły niezbędne dla oprogramowania Python zostały zawarte w pliku `Requirements.txt`.
Polecana metoda instalacji to wpisanie w lini komend systemu operacyjnego określonej operacji, gdzie zainstalowaliśmy język programowania Python.
```sh
> pip install -r Requirements.txt
```

### Działanie 
W załączonym folderze `BlobStorage` umieszczamy pobrane pliki ze strony https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page wybierając odpowiedni plik z miesiąca o nazwie "Yellow Taxi Trip Records", ilość plików zależy od naszego celu.

Następnie uruchamiamy program `EtlProcess.py`.

Celem końcowym będzie utworzenie folderu `ETLData` z odpowiednią strukturą podfolderów. Najbardziej istotnym będzie pliki w podfolderze `General` o nazwie `GeneralFile.xlsx`, gdyż on stanowi podłoże do tworzenia interaktywnego raportu.

## Interaktywny pulpit nawigacyjny
Raport został umieszczony wraz z plikiem na podstawie, którego został utworzony w folderze `Dashboard`. Jedynym elementem niezbędnym do uruchomienia go jest instalacja darmowej usługi Power BI Desktop.

### Wskazówka 
Wszystkie dane reagują na kliknięcie myszą odpowiedniej pozycji, np. wybieramy konretny dzień miesiąca na histogramie, a wszystkie dane zmieniają się w zależności od powiązania z nim.

Przyciski odnośnie miesiąca jedynie działają na zasadzie przytrzymania klawisza "Control" i kliknięcia lewym przyciskiem myszy.