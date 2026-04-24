# Bridge-DB Benchmark Report

Generated: 2026-04-14T08:16:31Z


## Benchmark Results

### Environment

| Property | Value |
|----------|-------|
| CPU | AMD Ryzen 5 7600X 6-Core Processor |
| Cores | 12 |
| RAM | 31GB |
| OS | linux/amd64 |
| Go | go1.25.7 |
| Disk | NVMe |

### Migration Performance

| Scenario | Size | Records | Data | Batch | Workers | Verify | Time | Rows/s | MB/s | Peak RAM |
|----------|------|--------:|-----:|------:|--------:|--------|-----:|-------:|-----:|---------:|
| sqlite→sqlite | large | 1M | 225MB | 1000 | 1 | false | 57.956s | 17254 | 10.48 | 552MB |
| sqlite→sqlite | large | 1M | 225MB | 1000 | 2 | false | 1m1.831s | 16173 | 9.82 | 567MB |
| sqlite→sqlite | large | 1M | 225MB | 1000 | 4 | false | 50.605s | 19761 | 12.00 | 596MB |
| sqlite→sqlite | large | 1M | 225MB | 1000 | 8 | false | 58.538s | 17083 | 10.38 | 608MB |
| sqlite→sqlite | large | 1M | 225MB | 1000 | 12 | false | 50.9s | 19646 | 11.93 | 636MB |
| sqlite→postgres | large | 1M | 225MB | 1000 | 1 | false | 37.361s | 26766 | 16.26 | 542MB |
| sqlite→postgres | large | 1M | 225MB | 1000 | 2 | false | 28.925s | 34572 | 21.00 | 531MB |
| sqlite→postgres | large | 1M | 225MB | 1000 | 4 | false | 30.952s | 32309 | 19.62 | 597MB |
| sqlite→postgres | large | 1M | 225MB | 1000 | 8 | false | 25.16s | 39746 | 24.14 | 662MB |
| sqlite→postgres | large | 1M | 225MB | 1000 | 12 | false | 23.908s | 41827 | 25.40 | 660MB |
| sqlite→mysql | large | 1M | 225MB | 1000 | 1 | false | 1m49.629s | 9121.6 | 5.54 | 506MB |
| sqlite→mysql | large | 1M | 225MB | 1000 | 2 | false | 1m4.932s | 15401 | 9.35 | 522MB |
| sqlite→mysql | large | 1M | 225MB | 1000 | 4 | false | 56.617s | 17663 | 10.73 | 599MB |
| sqlite→mysql | large | 1M | 225MB | 1000 | 8 | false | 1m6.858s | 14957 | 9.08 | 714MB |
| sqlite→mysql | large | 1M | 225MB | 1000 | 12 | false | 1m44.4s | 9578.5 | 5.82 | 647MB |
| sqlite→mariadb | large | 1M | 225MB | 1000 | 1 | false | 41.08s | 24343 | 14.79 | 516MB |
| sqlite→mariadb | large | 1M | 225MB | 1000 | 2 | false | 32.432s | 30834 | 18.73 | 523MB |
| sqlite→mariadb | large | 1M | 225MB | 1000 | 4 | false | 27.668s | 36143 | 21.95 | 571MB |
| sqlite→mariadb | large | 1M | 225MB | 1000 | 8 | false | 26.342s | 37962 | 23.06 | 594MB |
| sqlite→mariadb | large | 1M | 225MB | 1000 | 12 | false | 23.16s | 43178 | 26.23 | 622MB |
| sqlite→cockroachdb | large | 1M | 225MB | 1000 | 1 | false | 1m9.528s | 14383 | 8.74 | 516MB |
| sqlite→cockroachdb | large | 1M | 225MB | 1000 | 2 | false | 1m16.785s | 13023 | 7.91 | 518MB |
| sqlite→cockroachdb | large | 1M | 225MB | 1000 | 4 | false | 51.106s | 19567 | 11.88 | 580MB |
| sqlite→cockroachdb | large | 1M | 225MB | 1000 | 8 | false | 45.844s | 21813 | 13.25 | 653MB |
| sqlite→cockroachdb | large | 1M | 225MB | 1000 | 12 | false | 41.909s | 23861 | 14.49 | 703MB |
| sqlite→mssql | large | 1M | 225MB | 1000 | 1 | false | 12m36.852s | 1321.3 | 0.80 | 507MB |
| sqlite→mssql | large | 1M | 225MB | 1000 | 2 | false | 6m29.251s | 2569.0 | 1.56 | 518MB |
| sqlite→mssql | large | 1M | 225MB | 1000 | 4 | false | 3m26.971s | 4831.6 | 2.93 | 561MB |
| sqlite→mssql | large | 1M | 225MB | 1000 | 8 | false | 2m17.341s | 7281.1 | 4.42 | 642MB |
| sqlite→mssql | large | 1M | 225MB | 1000 | 12 | false | 1m55.585s | 8651.6 | 5.25 | 777MB |
| sqlite→mongodb | large | 1M | 225MB | 1000 | 1 | false | 42.347s | 23614 | 18.55 | 492MB |
| sqlite→mongodb | large | 1M | 225MB | 1000 | 2 | false | 27.33s | 36590 | 28.75 | 547MB |
| sqlite→mongodb | large | 1M | 225MB | 1000 | 4 | false | 18.701s | 53472 | 42.02 | 661MB |
| sqlite→mongodb | large | 1M | 225MB | 1000 | 8 | false | 19.724s | 50699 | 39.84 | 712MB |
| sqlite→mongodb | large | 1M | 225MB | 1000 | 12 | false | 16.947s | 59007 | 46.36 | 716MB |
| sqlite→redis | large | 1M | 225MB | 1000 | 1 | false | 15.445s | 64744 | 50.10 | 530MB |
| sqlite→redis | large | 1M | 225MB | 1000 | 2 | false | 14.514s | 68901 | 53.32 | 548MB |
| sqlite→redis | large | 1M | 225MB | 1000 | 4 | false | 13.728s | 72841 | 56.37 | 634MB |
| sqlite→redis | large | 1M | 225MB | 1000 | 8 | false | 12.506s | 79964 | 61.88 | 634MB |
| sqlite→redis | large | 1M | 225MB | 1000 | 12 | false | 11.295s | 88533 | 68.52 | 704MB |
| postgres→sqlite | large | 1M | 225MB | 1000 | 1 | false | 2m8.001s | 15011 | 14.26 | 525MB |
| postgres→sqlite | large | 1M | 225MB | 1000 | 2 | false | 1m52.701s | 17446 | 16.20 | 539MB |
| postgres→sqlite | large | 1M | 225MB | 1000 | 4 | false | 1m45.968s | 18590 | 17.23 | 568MB |
| postgres→sqlite | large | 1M | 225MB | 1000 | 8 | false | 1m44.721s | 18815 | 17.43 | 797MB |
| postgres→sqlite | large | 1M | 225MB | 1000 | 12 | false | 1m45.59s | 18660 | 17.29 | 794MB |
| postgres→postgres | large | 1M | 225MB | 1000 | 1 | false | 1m46.997s | 25473 | 24.26 | 601MB |
| postgres→postgres | large | 1M | 225MB | 1000 | 2 | false | 1m21.829s | 33802 | 31.72 | 530MB |
| postgres→postgres | large | 1M | 225MB | 1000 | 4 | false | 1m4.093s | 43199 | 40.50 | 570MB |
| postgres→postgres | large | 1M | 225MB | 1000 | 8 | false | 55.931s | 49520 | 46.41 | 692MB |
| postgres→postgres | large | 1M | 225MB | 1000 | 12 | false | 1m0.643s | 45705 | 42.81 | 798MB |
| postgres→mysql | large | 1M | 225MB | 1000 | 1 | false | 5m18.7s | 11062 | 10.56 | 577MB |
| postgres→mysql | large | 1M | 225MB | 1000 | 2 | false | 3m32.118s | 16829 | 15.87 | 555MB |
| postgres→mysql | large | 1M | 225MB | 1000 | 4 | false | 2m33.343s | 23299 | 21.95 | 608MB |
| postgres→mysql | large | 1M | 225MB | 1000 | 8 | false | 2m6.83s | 28189 | 26.54 | 746MB |
| postgres→mysql | large | 1M | 225MB | 1000 | 12 | false | 1m58.965s | 30061 | 28.30 | 737MB |
| postgres→mariadb | large | 1M | 225MB | 1000 | 1 | false | 3m10.782s | 22677 | 21.68 | 630MB |
| postgres→mariadb | large | 1M | 225MB | 1000 | 2 | false | 2m43.747s | 26517 | 25.26 | 558MB |
| postgres→mariadb | large | 1M | 225MB | 1000 | 4 | false | 2m26.847s | 29591 | 28.17 | 614MB |
| postgres→mariadb | large | 1M | 225MB | 1000 | 8 | false | 2m24.225s | 30138 | 28.68 | 613MB |
| postgres→mariadb | large | 1M | 225MB | 1000 | 12 | false | 2m22.893s | 30410 | 28.95 | 735MB |
| postgres→cockroachdb | large | 1M | 225MB | 1000 | 1 | false | 7m20.746s | 11627 | 11.13 | 606MB |
| postgres→cockroachdb | large | 1M | 225MB | 1000 | 2 | false | 4m39.31s | 18511 | 17.57 | 545MB |
| postgres→cockroachdb | large | 1M | 225MB | 1000 | 4 | false | 3m35.265s | 24052 | 22.80 | 648MB |
| postgres→cockroachdb | large | 1M | 225MB | 1000 | 8 | false | 3m34.886s | 24111 | 22.84 | 657MB |
| postgres→cockroachdb | large | 1M | 225MB | 1000 | 12 | false | 3m19.807s | 25933 | 24.56 | 699MB |
| postgres→mssql | large | 1M | 225MB | 1000 | 1 | false | 13m52.549s | 1201.4 | 6.76 | 522MB |
| postgres→mssql | large | 1M | 225MB | 1000 | 2 | false | 8m25.344s | 1986.6 | 11.14 | 535MB |
| postgres→mssql | large | 1M | 225MB | 1000 | 4 | false | 5m26.344s | 3078.8 | 17.25 | 605MB |
| postgres→mssql | large | 1M | 225MB | 1000 | 8 | false | 4m5.727s | 4090.3 | 22.91 | 694MB |
| postgres→mssql | large | 1M | 225MB | 1000 | 12 | false | 4m4.514s | 4111.0 | 23.03 | 777MB |
| postgres→mongodb | large | 1M | 225MB | 1000 | 1 | false | 4m38.221s | 14756 | 28.57 | 540MB |
| postgres→mongodb | large | 1M | 225MB | 1000 | 2 | false | 3m8.859s | 21921 | 42.20 | 569MB |
| postgres→mongodb | large | 1M | 225MB | 1000 | 4 | false | 2m22.164s | 29197 | 56.08 | 615MB |
| postgres→mongodb | large | 1M | 225MB | 1000 | 8 | false | 2m11.838s | 31491 | 60.47 | 629MB |
| postgres→mongodb | large | 1M | 225MB | 1000 | 12 | false | 2m4.405s | 33334 | 64.10 | 743MB |
| postgres→redis | large | 1M | 225MB | 1000 | 1 | false | 2m45.144s | 45497 | 35.10 | 566MB |
| postgres→redis | large | 1M | 225MB | 1000 | 2 | false | 2m30.484s | 50029 | 38.55 | 539MB |
| postgres→redis | large | 1M | 225MB | 1000 | 4 | false | 2m7.161s | 59272 | 45.65 | 661MB |
| postgres→redis | large | 1M | 225MB | 1000 | 8 | false | 2m0.359s | 62690 | 48.26 | 584MB |
| postgres→redis | large | 1M | 225MB | 1000 | 12 | false | 2m3.465s | 61036 | 47.01 | 693MB |
| mysql→sqlite | large | 1M | 225MB | 1000 | 1 | false | 1m6.115s | 15125 | 9.21 | 578MB |
| mysql→sqlite | large | 1M | 225MB | 1000 | 2 | false | 58.85s | 16992 | 10.35 | 522MB |
| mysql→sqlite | large | 1M | 225MB | 1000 | 4 | false | 55.024s | 18174 | 11.07 | 537MB |
| mysql→sqlite | large | 1M | 225MB | 1000 | 8 | false | 57.845s | 17288 | 10.53 | 702MB |
| mysql→sqlite | large | 1M | 225MB | 1000 | 12 | false | 57.83s | 17292 | 10.53 | 733MB |
| mysql→postgres | large | 1M | 225MB | 1000 | 1 | false | 37.738s | 26499 | 16.14 | 531MB |
| mysql→postgres | large | 1M | 225MB | 1000 | 2 | false | 29.721s | 33646 | 20.49 | 569MB |
| mysql→postgres | large | 1M | 225MB | 1000 | 4 | false | 35.381s | 28263 | 17.22 | 595MB |
| mysql→postgres | large | 1M | 225MB | 1000 | 8 | false | 24.845s | 40249 | 24.52 | 652MB |
| mysql→postgres | large | 1M | 225MB | 1000 | 12 | false | 24.523s | 40777 | 24.84 | 697MB |
| mysql→mysql | large | 1M | 225MB | 1000 | 1 | false | 1m37.632s | 10242 | 6.24 | 594MB |
| mysql→mysql | large | 1M | 225MB | 1000 | 2 | false | 1m6.669s | 14999 | 9.14 | 627MB |
| mysql→mysql | large | 1M | 225MB | 1000 | 4 | false | 1m1.74s | 16197 | 9.87 | 643MB |
| mysql→mysql | large | 1M | 225MB | 1000 | 8 | false | 1m2.258s | 16062 | 9.78 | 685MB |
| mysql→mysql | large | 1M | 225MB | 1000 | 12 | false | 58.893s | 16980 | 10.34 | 700MB |
| mysql→mariadb | large | 1M | 225MB | 1000 | 1 | false | 37.504s | 26663 | 16.24 | 521MB |
| mysql→mariadb | large | 1M | 225MB | 1000 | 2 | false | 32.594s | 30681 | 18.69 | 511MB |
| mysql→mariadb | large | 1M | 225MB | 1000 | 4 | false | 30.066s | 33261 | 20.26 | 591MB |
| mysql→mariadb | large | 1M | 225MB | 1000 | 8 | false | 26.869s | 37218 | 22.67 | 598MB |
| mysql→mariadb | large | 1M | 225MB | 1000 | 12 | false | 22.673s | 44106 | 26.87 | 757MB |
| mysql→cockroachdb | large | 1M | 225MB | 1000 | 1 | false | 2m8.311s | 7793.6 | 4.75 | 563MB |
| mysql→cockroachdb | large | 1M | 225MB | 1000 | 2 | false | 1m18.881s | 12677 | 7.72 | 516MB |
| mysql→cockroachdb | large | 1M | 225MB | 1000 | 4 | false | 53.657s | 18637 | 11.35 | 606MB |
| mysql→cockroachdb | large | 1M | 225MB | 1000 | 8 | false | 49.126s | 20356 | 12.40 | 677MB |
| mysql→cockroachdb | large | 1M | 225MB | 1000 | 12 | false | 46.529s | 21492 | 13.09 | 704MB |
| mysql→mssql | large | 1M | 225MB | 1000 | 1 | false | 12m45.707s | 1306.0 | 0.80 | 541MB |
| mysql→mssql | large | 1M | 225MB | 1000 | 2 | false | 6m34.518s | 2534.7 | 1.54 | 528MB |
| mysql→mssql | large | 1M | 225MB | 1000 | 4 | false | 4m22.87s | 3804.2 | 2.32 | 543MB |
| mysql→mssql | large | 1M | 225MB | 1000 | 8 | false | 2m3.885s | 8072.0 | 4.92 | 588MB |
| mysql→mssql | large | 1M | 225MB | 1000 | 12 | false | 1m53.395s | 8818.8 | 5.37 | 776MB |
| mysql→mongodb | large | 1M | 225MB | 1000 | 1 | false | 43.357s | 23064 | 17.91 | 539MB |
| mysql→mongodb | large | 1M | 225MB | 1000 | 2 | false | 28.741s | 34794 | 27.02 | 613MB |
| mysql→mongodb | large | 1M | 225MB | 1000 | 4 | false | 19.325s | 51746 | 40.18 | 613MB |
| mysql→mongodb | large | 1M | 225MB | 1000 | 8 | false | 17.74s | 56371 | 43.77 | 720MB |
| mysql→mongodb | large | 1M | 225MB | 1000 | 12 | false | 17.557s | 56959 | 44.22 | 753MB |
| mysql→redis | large | 1M | 225MB | 1000 | 1 | false | 15.514s | 64458 | 48.68 | 567MB |
| mysql→redis | large | 1M | 225MB | 1000 | 2 | false | 14.254s | 70155 | 52.99 | 534MB |
| mysql→redis | large | 1M | 225MB | 1000 | 4 | false | 12.851s | 77817 | 58.77 | 617MB |
| mysql→redis | large | 1M | 225MB | 1000 | 8 | false | 12.394s | 80684 | 60.94 | 618MB |
| mysql→redis | large | 1M | 225MB | 1000 | 12 | false | 11.259s | 88817 | 67.08 | 694MB |
| mariadb→sqlite | large | 1M | 225MB | 1000 | 1 | false | 1m8.602s | 14577 | 9.06 | 584MB |
| mariadb→sqlite | large | 1M | 225MB | 1000 | 2 | false | 58.778s | 17013 | 10.58 | 502MB |
| mariadb→sqlite | large | 1M | 225MB | 1000 | 4 | false | 58.889s | 16981 | 10.56 | 626MB |
| mariadb→sqlite | large | 1M | 225MB | 1000 | 8 | false | 56.724s | 17629 | 10.96 | 693MB |
| mariadb→sqlite | large | 1M | 225MB | 1000 | 12 | false | 59.174s | 16899 | 10.51 | 725MB |
| mariadb→postgres | large | 1M | 225MB | 1000 | 1 | false | 41.126s | 24315 | 15.12 | 515MB |
| mariadb→postgres | large | 1M | 225MB | 1000 | 2 | false | 31.56s | 31686 | 19.70 | 532MB |
| mariadb→postgres | large | 1M | 225MB | 1000 | 4 | false | 29.996s | 33337 | 20.73 | 569MB |
| mariadb→postgres | large | 1M | 225MB | 1000 | 8 | false | 24.845s | 40249 | 25.02 | 580MB |
| mariadb→postgres | large | 1M | 225MB | 1000 | 12 | false | 23.904s | 41835 | 26.01 | 693MB |
| mariadb→mysql | large | 1M | 225MB | 1000 | 1 | false | 1m44.165s | 9600.2 | 5.97 | 555MB |
| mariadb→mysql | large | 1M | 225MB | 1000 | 2 | false | 1m5.95s | 15163 | 9.43 | 512MB |
| mariadb→mysql | large | 1M | 225MB | 1000 | 4 | false | 1m1.989s | 16132 | 10.03 | 579MB |
| mariadb→mysql | large | 1M | 225MB | 1000 | 8 | false | 1m2.312s | 16048 | 9.98 | 651MB |
| mariadb→mysql | large | 1M | 225MB | 1000 | 12 | false | 1m4.123s | 15595 | 9.70 | 737MB |
| mariadb→mariadb | large | 1M | 225MB | 1000 | 1 | false | 1m18.794s | 12691 | 7.89 | 624MB |
| mariadb→mariadb | large | 1M | 225MB | 1000 | 2 | false | 55.642s | 17972 | 11.17 | 588MB |
| mariadb→mariadb | large | 1M | 225MB | 1000 | 4 | false | 26.359s | 37938 | 23.59 | 647MB |
| mariadb→mariadb | large | 1M | 225MB | 1000 | 8 | false | 25.045s | 39928 | 24.82 | 635MB |
| mariadb→mariadb | large | 1M | 225MB | 1000 | 12 | false | 21.92s | 45621 | 28.36 | 652MB |
| mariadb→cockroachdb | large | 1M | 225MB | 1000 | 1 | false | 2m8.383s | 7789.2 | 4.84 | 529MB |
| mariadb→cockroachdb | large | 1M | 225MB | 1000 | 2 | false | 1m18.851s | 12682 | 7.88 | 519MB |
| mariadb→cockroachdb | large | 1M | 225MB | 1000 | 4 | false | 57.877s | 17278 | 10.74 | 580MB |
| mariadb→cockroachdb | large | 1M | 225MB | 1000 | 8 | false | 45.555s | 21952 | 13.65 | 656MB |
| mariadb→cockroachdb | large | 1M | 225MB | 1000 | 12 | false | 43.436s | 23023 | 14.31 | 734MB |
| mariadb→mssql | large | 1M | 225MB | 1000 | 1 | false | 13m27.575s | 1238.3 | 0.77 | 545MB |
| mariadb→mssql | large | 1M | 225MB | 1000 | 2 | false | 6m52.146s | 2426.3 | 1.51 | 528MB |
| mariadb→mssql | large | 1M | 225MB | 1000 | 4 | false | 3m35.17s | 4647.5 | 2.89 | 533MB |
| mariadb→mssql | large | 1M | 225MB | 1000 | 8 | false | 2m31.323s | 6608.4 | 4.11 | 621MB |
| mariadb→mssql | large | 1M | 225MB | 1000 | 12 | false | 1m55.144s | 8684.8 | 5.40 | 760MB |
| mariadb→mongodb | large | 1M | 225MB | 1000 | 1 | false | 45.681s | 21891 | 17.00 | 524MB |
| mariadb→mongodb | large | 1M | 225MB | 1000 | 2 | false | 45.537s | 21960 | 17.05 | 540MB |
| mariadb→mongodb | large | 1M | 225MB | 1000 | 4 | false | 30.975s | 32284 | 25.07 | 588MB |
| mariadb→mongodb | large | 1M | 225MB | 1000 | 8 | false | 28.459s | 35138 | 27.28 | 578MB |
| mariadb→mongodb | large | 1M | 225MB | 1000 | 12 | false | 29.935s | 33406 | 25.94 | 660MB |
| mariadb→redis | large | 1M | 225MB | 1000 | 1 | false | 34.915s | 28641 | 21.63 | 505MB |
| mariadb→redis | large | 1M | 225MB | 1000 | 2 | false | 26.574s | 37631 | 28.42 | 569MB |
| mariadb→redis | large | 1M | 225MB | 1000 | 4 | false | 24.9s | 40160 | 30.33 | 544MB |
| mariadb→redis | large | 1M | 225MB | 1000 | 8 | false | 25.147s | 39766 | 30.04 | 564MB |
| mariadb→redis | large | 1M | 225MB | 1000 | 12 | false | 24.734s | 40430 | 30.54 | 553MB |
| cockroachdb→sqlite | large | 1M | 225MB | 1000 | 1 | false | 1m41.053s | 13359 | 12.19 | 712MB |
| cockroachdb→sqlite | large | 1M | 225MB | 1000 | 2 | false | 1m29.945s | 15009 | 13.69 | 771MB |
| cockroachdb→sqlite | large | 1M | 225MB | 1000 | 4 | false | 1m17.784s | 17356 | 15.84 | 836MB |
| cockroachdb→sqlite | large | 1M | 225MB | 1000 | 8 | false | 1m24.6s | 15958 | 14.56 | 836MB |
| cockroachdb→sqlite | large | 1M | 225MB | 1000 | 12 | false | 1m18.185s | 17267 | 15.75 | 907MB |
| cockroachdb→postgres | large | 1M | 225MB | 1000 | 1 | false | 2m6.391s | 17034 | 15.82 | 995MB |
| cockroachdb→postgres | large | 1M | 225MB | 1000 | 2 | false | 1m51.663s | 19280 | 17.91 | 1.1GB |
| cockroachdb→postgres | large | 1M | 225MB | 1000 | 4 | false | 1m37.623s | 22053 | 20.49 | 1.2GB |
| cockroachdb→postgres | large | 1M | 225MB | 1000 | 8 | false | 1m28.122s | 24431 | 22.69 | 1.3GB |
| cockroachdb→postgres | large | 1M | 225MB | 1000 | 12 | false | 1m11.67s | 30039 | 27.90 | 1.4GB |
| cockroachdb→mysql | large | 1M | 225MB | 1000 | 1 | false | 6m0.822s | 8191.9 | 7.67 | 1.2GB |
| cockroachdb→mysql | large | 1M | 225MB | 1000 | 2 | false | 4m13.242s | 11672 | 10.93 | 1.1GB |
| cockroachdb→mysql | large | 1M | 225MB | 1000 | 4 | false | 4m20.355s | 11353 | 10.63 | 1.3GB |
| cockroachdb→mysql | large | 1M | 225MB | 1000 | 8 | false | 4m45.703s | 10346 | 9.69 | 1.5GB |
| cockroachdb→mysql | large | 1M | 225MB | 1000 | 12 | false | 4m43.847s | 10413 | 9.75 | 1.6GB |
| cockroachdb→mariadb | large | 1M | 225MB | 1000 | 1 | false | 4m14.618s | 14762 | 13.89 | 1.3GB |
| cockroachdb→mariadb | large | 1M | 225MB | 1000 | 2 | false | 3m47.919s | 16492 | 15.51 | 1.3GB |
| cockroachdb→mariadb | large | 1M | 225MB | 1000 | 4 | false | 3m31.489s | 17773 | 16.72 | 1.4GB |
| cockroachdb→mariadb | large | 1M | 225MB | 1000 | 8 | false | 3m16.35s | 19143 | 18.01 | 1.5GB |
| cockroachdb→mariadb | large | 1M | 225MB | 1000 | 12 | false | 2m40.825s | 23372 | 21.99 | 1.6GB |
| cockroachdb→cockroachdb | large | 1M | 225MB | 1000 | 1 | false | 8m24.584s | 9040.4 | 8.53 | 1.1GB |
| cockroachdb→cockroachdb | large | 1M | 225MB | 1000 | 2 | false | 6m48.327s | 11172 | 10.54 | 1.2GB |
| cockroachdb→cockroachdb | large | 1M | 225MB | 1000 | 4 | false | 4m47s | 15894 | 15.00 | 1.2GB |
| cockroachdb→cockroachdb | large | 1M | 225MB | 1000 | 8 | false | 4m43.703s | 16079 | 15.17 | 1.3GB |
| cockroachdb→cockroachdb | large | 1M | 225MB | 1000 | 12 | false | 3m51.616s | 19695 | 18.58 | 1.3GB |
| cockroachdb→mongodb | large | 1M | 225MB | 1000 | 1 | false | 7m39.302s | 9084.8 | 15.79 | 1.1GB |
| cockroachdb→mongodb | large | 1M | 225MB | 1000 | 2 | false | 6m38.34s | 10492 | 18.21 | 1.4GB |
| cockroachdb→mongodb | large | 1M | 225MB | 1000 | 4 | false | 5m42.827s | 12234 | 21.16 | 1.4GB |
| cockroachdb→mongodb | large | 1M | 225MB | 1000 | 8 | false | 5m29.752s | 12793 | 21.99 | 1.4GB |
| cockroachdb→mongodb | large | 1M | 225MB | 1000 | 12 | false | 5m9.743s | 13635 | 23.42 | 1.6GB |
| cockroachdb→redis | large | 1M | 225MB | 1000 | 1 | false | 7m30.691s | 15466 | 11.88 | 1.2GB |
| cockroachdb→redis | large | 1M | 225MB | 1000 | 2 | false | 6m26.866s | 18018 | 13.84 | 1.4GB |
| cockroachdb→redis | large | 1M | 225MB | 1000 | 4 | false | 5m36.683s | 20703 | 15.90 | 1.4GB |
| cockroachdb→redis | large | 1M | 225MB | 1000 | 8 | false | 5m16.937s | 21993 | 16.90 | 1.4GB |
| mssql→sqlite | large | 1M | 225MB | 1000 | 1 | false | 1m7.496s | 14816 | 9.16 | 556MB |
| mssql→sqlite | large | 1M | 225MB | 1000 | 2 | false | 58.076s | 17219 | 10.65 | 573MB |
| mssql→sqlite | large | 1M | 225MB | 1000 | 4 | false | 58.23s | 17173 | 10.62 | 565MB |
| mssql→sqlite | large | 1M | 225MB | 1000 | 8 | false | 58.136s | 17201 | 10.64 | 646MB |
| mssql→sqlite | large | 1M | 225MB | 1000 | 12 | false | 57.309s | 17449 | 10.79 | 681MB |
| mssql→postgres | large | 1M | 225MB | 1000 | 1 | false | 42.645s | 23449 | 14.50 | 555MB |
| mssql→postgres | large | 1M | 225MB | 1000 | 2 | false | 32.673s | 30606 | 18.93 | 575MB |
| mssql→postgres | large | 1M | 225MB | 1000 | 4 | false | 28.138s | 35540 | 21.98 | 627MB |
| mssql→postgres | large | 1M | 225MB | 1000 | 8 | false | 28.518s | 35066 | 21.69 | 716MB |
| mssql→postgres | large | 1M | 225MB | 1000 | 12 | false | 25.254s | 39598 | 24.49 | 731MB |
| mssql→mysql | large | 1M | 225MB | 1000 | 1 | false | 1m41.356s | 9866.3 | 6.10 | 544MB |
| mssql→mysql | large | 1M | 225MB | 1000 | 2 | false | 1m5.73s | 15214 | 9.41 | 544MB |
| mssql→mysql | large | 1M | 225MB | 1000 | 4 | false | 1m3.897s | 15650 | 9.68 | 695MB |
| mssql→mysql | large | 1M | 225MB | 1000 | 8 | false | 1m2.442s | 16015 | 9.90 | 801MB |
| mssql→mysql | large | 1M | 225MB | 1000 | 12 | false | 1m0.306s | 16582 | 10.25 | 768MB |
| mssql→mariadb | large | 1M | 225MB | 1000 | 1 | false | 40.283s | 24824 | 15.35 | 580MB |
| mssql→mariadb | large | 1M | 225MB | 1000 | 2 | false | 36.756s | 27206 | 16.82 | 572MB |
| mssql→mariadb | large | 1M | 225MB | 1000 | 4 | false | 31.551s | 31695 | 19.60 | 614MB |
| mssql→mariadb | large | 1M | 225MB | 1000 | 8 | false | 28.889s | 34615 | 21.41 | 641MB |
| mssql→mariadb | large | 1M | 225MB | 1000 | 12 | false | 26.009s | 38448 | 23.78 | 760MB |
| mssql→cockroachdb | large | 1M | 225MB | 1000 | 1 | false | 2m20.254s | 7129.9 | 4.41 | 599MB |
| mssql→cockroachdb | large | 1M | 225MB | 1000 | 2 | false | 2m25.388s | 6878.1 | 4.25 | 556MB |
| mssql→cockroachdb | large | 1M | 225MB | 1000 | 4 | false | 1m0.218s | 16606 | 10.27 | 616MB |
| mssql→cockroachdb | large | 1M | 225MB | 1000 | 8 | false | 52.135s | 19181 | 11.86 | 642MB |
| mssql→cockroachdb | large | 1M | 225MB | 1000 | 12 | false | 48.832s | 20478 | 12.66 | 784MB |
| mssql→mssql | large | 1M | 225MB | 1000 | 1 | false | 12m47.649s | 1302.7 | 0.81 | 551MB |
| mssql→mssql | large | 1M | 225MB | 1000 | 2 | false | 6m32.817s | 2545.7 | 1.57 | 522MB |
| mssql→mssql | large | 1M | 225MB | 1000 | 4 | false | 22m5.687s | 754.3 | 0.47 | 538MB |
| mssql→mssql | large | 1M | 225MB | 1000 | 8 | false | 2m18.02s | 7245.3 | 4.48 | 636MB |
| mssql→mssql | large | 1M | 225MB | 1000 | 12 | false | 2m1.673s | 8218.8 | 5.08 | 798MB |
| mssql→mongodb | large | 1M | 225MB | 1000 | 1 | false | 46.007s | 21736 | 17.08 | 552MB |
| mssql→mongodb | large | 1M | 225MB | 1000 | 2 | false | 33.261s | 30065 | 23.62 | 591MB |
| mssql→mongodb | large | 1M | 225MB | 1000 | 4 | false | 35.259s | 28361 | 22.28 | 627MB |
| mssql→mongodb | large | 1M | 225MB | 1000 | 8 | false | 30.466s | 32824 | 25.79 | 611MB |
| mssql→mongodb | large | 1M | 225MB | 1000 | 12 | false | 30.138s | 33180 | 26.07 | 632MB |
| mongodb→sqlite | large | 1M | 225MB | 1000 | 1 | false | 1m5.819s | 15193 | 9.64 | 559MB |
| mongodb→sqlite | large | 1M | 225MB | 1000 | 2 | false | 1m0.005s | 16665 | 10.58 | 590MB |
| mongodb→sqlite | large | 1M | 225MB | 1000 | 4 | false | 57.373s | 17430 | 11.06 | 586MB |
| mongodb→sqlite | large | 1M | 225MB | 1000 | 8 | false | 57.303s | 17451 | 11.08 | 659MB |
| mongodb→sqlite | large | 1M | 225MB | 1000 | 12 | false | 57.206s | 17481 | 11.10 | 767MB |
| mongodb→postgres | large | 1M | 225MB | 1000 | 1 | false | 36.834s | 21799 | 17.64 | 653MB |
| mongodb→postgres | large | 1M | 225MB | 1000 | 2 | false | 27.725s | 28960 | 23.43 | 563MB |
| mongodb→postgres | large | 1M | 225MB | 1000 | 4 | false | 23.833s | 33689 | 27.26 | 567MB |
| mongodb→postgres | large | 1M | 225MB | 1000 | 8 | false | 21.215s | 37847 | 30.62 | 715MB |
| mongodb→postgres | large | 1M | 225MB | 1000 | 12 | false | 26.715s | 30055 | 24.32 | 824MB |
| mongodb→mysql | large | 1M | 225MB | 1000 | 1 | false | 1m49.861s | 9102.4 | 5.78 | 553MB |
| mongodb→mysql | large | 1M | 225MB | 1000 | 2 | false | 1m37.754s | 10230 | 6.49 | 543MB |
| mongodb→mysql | large | 1M | 225MB | 1000 | 4 | false | 1m37.5s | 10256 | 6.51 | 570MB |
| mongodb→mysql | large | 1M | 225MB | 1000 | 8 | false | 1m35.264s | 10497 | 6.66 | 622MB |
| mongodb→mysql | large | 1M | 225MB | 1000 | 12 | false | 1m38.062s | 10198 | 6.47 | 848MB |
| mongodb→mariadb | large | 1M | 225MB | 1000 | 1 | false | 46.65s | 21436 | 13.61 | 540MB |
| mongodb→mariadb | large | 1M | 225MB | 1000 | 2 | false | 36.7s | 27248 | 17.30 | 579MB |
| mongodb→mariadb | large | 1M | 225MB | 1000 | 4 | false | 30.433s | 32859 | 20.86 | 567MB |
| mongodb→mariadb | large | 1M | 225MB | 1000 | 8 | false | 29.742s | 33623 | 21.34 | 745MB |
| mongodb→mariadb | large | 1M | 225MB | 1000 | 12 | false | 26.078s | 38346 | 24.34 | 883MB |
| mongodb→cockroachdb | large | 1M | 225MB | 1000 | 1 | false | 1m3.665s | 12612 | 10.20 | 602MB |
| mongodb→cockroachdb | large | 1M | 225MB | 1000 | 2 | false | 45.074s | 17813 | 14.41 | 559MB |
| mongodb→cockroachdb | large | 1M | 225MB | 1000 | 4 | false | 33.001s | 24331 | 19.69 | 571MB |
| mongodb→cockroachdb | large | 1M | 225MB | 1000 | 8 | false | 33.195s | 24188 | 19.57 | 621MB |
| mongodb→cockroachdb | large | 1M | 225MB | 1000 | 12 | false | 33.137s | 24230 | 19.61 | 749MB |
| mongodb→mssql | large | 1M | 225MB | 1000 | 1 | false | 18m41.365s | 891.8 | 0.57 | 519MB |
| mongodb→mssql | large | 1M | 225MB | 1000 | 2 | false | 9m51.924s | 1689.4 | 1.07 | 525MB |
| mongodb→mssql | large | 1M | 225MB | 1000 | 4 | false | 6m26.471s | 2587.5 | 1.64 | 544MB |
| mongodb→mssql | large | 1M | 225MB | 1000 | 8 | false | 5m45.424s | 2895.0 | 1.84 | 613MB |
| mongodb→mssql | large | 1M | 225MB | 1000 | 12 | false | 5m41.021s | 2932.4 | 1.86 | 768MB |
| mongodb→mongodb | large | 1M | 225MB | 1000 | 1 | false | 35.481s | 28184 | 20.37 | 589MB |
| mongodb→mongodb | large | 1M | 225MB | 1000 | 2 | false | 24.005s | 41657 | 30.10 | 548MB |
| mongodb→mongodb | large | 1M | 225MB | 1000 | 4 | false | 20.403s | 49012 | 35.42 | 626MB |
| mongodb→mongodb | large | 1M | 225MB | 1000 | 8 | false | 19.909s | 50228 | 36.30 | 626MB |
| mongodb→mongodb | large | 1M | 225MB | 1000 | 12 | false | 18.694s | 53493 | 38.66 | 707MB |
| redis→sqlite | large | 1M | 225MB | 1000 | 1 | false | 1m24.928s | 11775 | 7.72 | 834MB |
| redis→sqlite | large | 1M | 225MB | 1000 | 2 | false | 1m23.044s | 12042 | 7.90 | 933MB |
| redis→sqlite | large | 1M | 225MB | 1000 | 4 | false | 1m23.518s | 11974 | 7.85 | 929MB |
| redis→sqlite | large | 1M | 225MB | 1000 | 8 | false | 1m23.652s | 11954 | 7.84 | 869MB |
| redis→sqlite | large | 1M | 225MB | 1000 | 12 | false | 1m23.107s | 12033 | 7.89 | 894MB |
| redis→postgres | large | 1M | 225MB | 1000 | 1 | false | 1m26.974s | 11498 | 7.74 | 779MB |
| redis→postgres | large | 1M | 225MB | 1000 | 2 | false | 1m25.577s | 11685 | 7.86 | 836MB |
| redis→postgres | large | 1M | 225MB | 1000 | 4 | false | 1m24.93s | 11774 | 7.92 | 831MB |
| redis→postgres | large | 1M | 225MB | 1000 | 8 | false | 1m24.432s | 11844 | 7.97 | 850MB |
| redis→postgres | large | 1M | 225MB | 1000 | 12 | false | 1m24.587s | 11822 | 7.96 | 848MB |
| redis→mysql | large | 1M | 225MB | 1000 | 1 | false | 1m22.837s | 0.00 | 7.79 | 821MB |
| redis→mysql | large | 1M | 225MB | 1000 | 2 | false | 1m22.037s | 0.00 | 7.86 | 784MB |
| redis→mysql | large | 1M | 225MB | 1000 | 4 | false | 1m22.056s | 0.00 | 7.86 | 800MB |
| redis→mysql | large | 1M | 225MB | 1000 | 8 | false | 1m21.999s | 0.00 | 7.87 | 824MB |
| redis→mysql | large | 1M | 225MB | 1000 | 12 | false | 1m22.171s | 0.00 | 7.85 | 839MB |
| redis→mariadb | large | 1M | 225MB | 1000 | 1 | false | 1m22.384s | 0.00 | 7.83 | 785MB |
| redis→mariadb | large | 1M | 225MB | 1000 | 2 | false | 1m22.003s | 0.00 | 7.87 | 795MB |
| redis→mariadb | large | 1M | 225MB | 1000 | 4 | false | 1m22.114s | 0.00 | 7.86 | 781MB |
| redis→mariadb | large | 1M | 225MB | 1000 | 8 | false | 1m22.099s | 0.00 | 7.86 | 847MB |
| redis→mariadb | large | 1M | 225MB | 1000 | 12 | false | 1m21.753s | 0.00 | 7.89 | 771MB |
| redis→cockroachdb | large | 1M | 225MB | 1000 | 1 | false | 7m18.557s | 2280.2 | 1.53 | 855MB |
| redis→cockroachdb | large | 1M | 225MB | 1000 | 2 | false | 4m36.261s | 3619.8 | 2.44 | 848MB |
| redis→cockroachdb | large | 1M | 225MB | 1000 | 4 | false | 2m52.953s | 5781.9 | 3.89 | 865MB |
| redis→cockroachdb | large | 1M | 225MB | 1000 | 8 | false | 2m18.464s | 7222.1 | 4.86 | 1.1GB |
| redis→cockroachdb | large | 1M | 225MB | 1000 | 12 | false | 2m19.96s | 7144.9 | 4.81 | 1010MB |
| redis→mongodb | large | 1M | 225MB | 1000 | 1 | false | 1m23.706s | 11947 | 9.87 | 817MB |
| redis→mongodb | large | 1M | 225MB | 1000 | 2 | false | 1m22.943s | 12056 | 9.96 | 821MB |
| redis→mongodb | large | 1M | 225MB | 1000 | 4 | false | 1m22.894s | 12064 | 9.96 | 795MB |
| redis→mongodb | large | 1M | 225MB | 1000 | 8 | false | 1m22.886s | 12065 | 9.96 | 830MB |
| redis→mongodb | large | 1M | 225MB | 1000 | 12 | false | 1m22.749s | 12085 | 9.98 | 842MB |

### Parameter Analysis

#### large dataset (1M records)

**cockroachdb→cockroachdb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 9040.4 | - | 1.1GB |
| 2 | 0.17x | 11172 | 1.24x | 1.2GB |
| 4 | 0.33x | 15894 | 1.76x | 1.2GB |
| 8 | 0.67x | 16079 | 1.78x | 1.3GB |
| 12 | 1.00x | 19695 | 2.18x | 1.3GB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**cockroachdb→mariadb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 14762 | - | 1.3GB |
| 2 | 0.17x | 16492 | 1.12x | 1.3GB |
| 4 | 0.33x | 17773 | 1.20x | 1.4GB |
| 8 | 0.67x | 19143 | 1.30x | 1.5GB |
| 12 | 1.00x | 23372 | 1.58x | 1.6GB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**cockroachdb→mongodb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 9084.8 | - | 1.1GB |
| 2 | 0.17x | 10492 | 1.15x | 1.4GB |
| 4 | 0.33x | 12234 | 1.35x | 1.4GB |
| 8 | 0.67x | 12793 | 1.41x | 1.4GB |
| 12 | 1.00x | 13635 | 1.50x | 1.6GB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**cockroachdb→mysql** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 8191.9 | - | 1.2GB |
| 2 | 0.17x | 11672 | 1.42x | 1.1GB |
| 4 | 0.33x | 11353 | 1.39x | 1.3GB |
| 8 | 0.67x | 10346 | 1.26x | 1.5GB |
| 12 | 1.00x | 10413 | 1.27x | 1.6GB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**cockroachdb→postgres** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 17034 | - | 995MB |
| 2 | 0.17x | 19280 | 1.13x | 1.1GB |
| 4 | 0.33x | 22053 | 1.29x | 1.2GB |
| 8 | 0.67x | 24431 | 1.43x | 1.3GB |
| 12 | 1.00x | 30039 | 1.76x | 1.4GB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**cockroachdb→redis** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 15466 | - | 1.2GB |
| 2 | 0.17x | 18018 | 1.16x | 1.4GB |
| 4 | 0.33x | 20703 | 1.34x | 1.4GB |
| 8 | 0.67x | 21993 | 1.42x | 1.4GB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**cockroachdb→sqlite** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 13359 | - | 712MB |
| 2 | 0.17x | 15009 | 1.12x | 771MB |
| 4 | 0.33x | 17356 | 1.30x | 836MB |
| 8 | 0.67x | 15958 | 1.19x | 836MB |
| 12 | 1.00x | 17267 | 1.29x | 907MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mariadb→cockroachdb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 7789.2 | - | 529MB |
| 2 | 0.17x | 12682 | 1.63x | 519MB |
| 4 | 0.33x | 17278 | 2.22x | 580MB |
| 8 | 0.67x | 21952 | 2.82x | 656MB |
| 12 | 1.00x | 23023 | 2.96x | 734MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mariadb→mariadb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 12691 | - | 624MB |
| 2 | 0.17x | 17972 | 1.42x | 588MB |
| 4 | 0.33x | 37938 | 2.99x | 647MB |
| 8 | 0.67x | 39928 | 3.15x | 635MB |
| 12 | 1.00x | 45621 | 3.59x | 652MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mariadb→mongodb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 21891 | - | 524MB |
| 2 | 0.17x | 21960 | 1.00x | 540MB |
| 4 | 0.33x | 32284 | 1.47x | 588MB |
| 8 | 0.67x | 35138 | 1.61x | 578MB |
| 12 | 1.00x | 33406 | 1.53x | 660MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mariadb→mssql** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 1238.3 | - | 545MB |
| 2 | 0.17x | 2426.3 | 1.96x | 528MB |
| 4 | 0.33x | 4647.5 | 3.75x | 533MB |
| 8 | 0.67x | 6608.4 | 5.34x | 621MB |
| 12 | 1.00x | 8684.8 | 7.01x | 760MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mariadb→mysql** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 9600.2 | - | 555MB |
| 2 | 0.17x | 15163 | 1.58x | 512MB |
| 4 | 0.33x | 16132 | 1.68x | 579MB |
| 8 | 0.67x | 16048 | 1.67x | 651MB |
| 12 | 1.00x | 15595 | 1.62x | 737MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mariadb→postgres** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 24315 | - | 515MB |
| 2 | 0.17x | 31686 | 1.30x | 532MB |
| 4 | 0.33x | 33337 | 1.37x | 569MB |
| 8 | 0.67x | 40249 | 1.66x | 580MB |
| 12 | 1.00x | 41835 | 1.72x | 693MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mariadb→redis** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 28641 | - | 505MB |
| 2 | 0.17x | 37631 | 1.31x | 569MB |
| 4 | 0.33x | 40160 | 1.40x | 544MB |
| 8 | 0.67x | 39766 | 1.39x | 564MB |
| 12 | 1.00x | 40430 | 1.41x | 553MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mariadb→sqlite** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 14577 | - | 584MB |
| 2 | 0.17x | 17013 | 1.17x | 502MB |
| 4 | 0.33x | 16981 | 1.16x | 626MB |
| 8 | 0.67x | 17629 | 1.21x | 693MB |
| 12 | 1.00x | 16899 | 1.16x | 725MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mongodb→cockroachdb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 12612 | - | 602MB |
| 2 | 0.17x | 17813 | 1.41x | 559MB |
| 4 | 0.33x | 24331 | 1.93x | 571MB |
| 8 | 0.67x | 24188 | 1.92x | 621MB |
| 12 | 1.00x | 24230 | 1.92x | 749MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mongodb→mariadb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 21436 | - | 540MB |
| 2 | 0.17x | 27248 | 1.27x | 579MB |
| 4 | 0.33x | 32859 | 1.53x | 567MB |
| 8 | 0.67x | 33623 | 1.57x | 745MB |
| 12 | 1.00x | 38346 | 1.79x | 883MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mongodb→mongodb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 28184 | - | 589MB |
| 2 | 0.17x | 41657 | 1.48x | 548MB |
| 4 | 0.33x | 49012 | 1.74x | 626MB |
| 8 | 0.67x | 50228 | 1.78x | 626MB |
| 12 | 1.00x | 53493 | 1.90x | 707MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mongodb→mssql** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 891.8 | - | 519MB |
| 2 | 0.17x | 1689.4 | 1.89x | 525MB |
| 4 | 0.33x | 2587.5 | 2.90x | 544MB |
| 8 | 0.67x | 2895.0 | 3.25x | 613MB |
| 12 | 1.00x | 2932.4 | 3.29x | 768MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mongodb→mysql** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 9102.4 | - | 553MB |
| 2 | 0.17x | 10230 | 1.12x | 543MB |
| 4 | 0.33x | 10256 | 1.13x | 570MB |
| 8 | 0.67x | 10497 | 1.15x | 622MB |
| 12 | 1.00x | 10198 | 1.12x | 848MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mongodb→postgres** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 21799 | - | 653MB |
| 2 | 0.17x | 28960 | 1.33x | 563MB |
| 4 | 0.33x | 33689 | 1.55x | 567MB |
| 8 | 0.67x | 37847 | 1.74x | 715MB |
| 12 | 1.00x | 30055 | 1.38x | 824MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mongodb→sqlite** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 15193 | - | 559MB |
| 2 | 0.17x | 16665 | 1.10x | 590MB |
| 4 | 0.33x | 17430 | 1.15x | 586MB |
| 8 | 0.67x | 17451 | 1.15x | 659MB |
| 12 | 1.00x | 17481 | 1.15x | 767MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mssql→cockroachdb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 7129.9 | - | 599MB |
| 2 | 0.17x | 6878.1 | 0.96x | 556MB |
| 4 | 0.33x | 16606 | 2.33x | 616MB |
| 8 | 0.67x | 19181 | 2.69x | 642MB |
| 12 | 1.00x | 20478 | 2.87x | 784MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mssql→mariadb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 24824 | - | 580MB |
| 2 | 0.17x | 27206 | 1.10x | 572MB |
| 4 | 0.33x | 31695 | 1.28x | 614MB |
| 8 | 0.67x | 34615 | 1.39x | 641MB |
| 12 | 1.00x | 38448 | 1.55x | 760MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mssql→mongodb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 21736 | - | 552MB |
| 2 | 0.17x | 30065 | 1.38x | 591MB |
| 4 | 0.33x | 28361 | 1.30x | 627MB |
| 8 | 0.67x | 32824 | 1.51x | 611MB |
| 12 | 1.00x | 33180 | 1.53x | 632MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mssql→mssql** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 1302.7 | - | 551MB |
| 2 | 0.17x | 2545.7 | 1.95x | 522MB |
| 4 | 0.33x | 754.3 | 0.58x | 538MB |
| 8 | 0.67x | 7245.3 | 5.56x | 636MB |
| 12 | 1.00x | 8218.8 | 6.31x | 798MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mssql→mysql** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 9866.3 | - | 544MB |
| 2 | 0.17x | 15214 | 1.54x | 544MB |
| 4 | 0.33x | 15650 | 1.59x | 695MB |
| 8 | 0.67x | 16015 | 1.62x | 801MB |
| 12 | 1.00x | 16582 | 1.68x | 768MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mssql→postgres** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 23449 | - | 555MB |
| 2 | 0.17x | 30606 | 1.31x | 575MB |
| 4 | 0.33x | 35540 | 1.52x | 627MB |
| 8 | 0.67x | 35066 | 1.50x | 716MB |
| 12 | 1.00x | 39598 | 1.69x | 731MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mssql→sqlite** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 14816 | - | 556MB |
| 2 | 0.17x | 17219 | 1.16x | 573MB |
| 4 | 0.33x | 17173 | 1.16x | 565MB |
| 8 | 0.67x | 17201 | 1.16x | 646MB |
| 12 | 1.00x | 17449 | 1.18x | 681MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mysql→cockroachdb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 7793.6 | - | 563MB |
| 2 | 0.17x | 12677 | 1.63x | 516MB |
| 4 | 0.33x | 18637 | 2.39x | 606MB |
| 8 | 0.67x | 20356 | 2.61x | 677MB |
| 12 | 1.00x | 21492 | 2.76x | 704MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mysql→mariadb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 26663 | - | 521MB |
| 2 | 0.17x | 30681 | 1.15x | 511MB |
| 4 | 0.33x | 33261 | 1.25x | 591MB |
| 8 | 0.67x | 37218 | 1.40x | 598MB |
| 12 | 1.00x | 44106 | 1.65x | 757MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mysql→mongodb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 23064 | - | 539MB |
| 2 | 0.17x | 34794 | 1.51x | 613MB |
| 4 | 0.33x | 51746 | 2.24x | 613MB |
| 8 | 0.67x | 56371 | 2.44x | 720MB |
| 12 | 1.00x | 56959 | 2.47x | 753MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mysql→mssql** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 1306.0 | - | 541MB |
| 2 | 0.17x | 2534.7 | 1.94x | 528MB |
| 4 | 0.33x | 3804.2 | 2.91x | 543MB |
| 8 | 0.67x | 8072.0 | 6.18x | 588MB |
| 12 | 1.00x | 8818.8 | 6.75x | 776MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mysql→mysql** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 10242 | - | 594MB |
| 2 | 0.17x | 14999 | 1.46x | 627MB |
| 4 | 0.33x | 16197 | 1.58x | 643MB |
| 8 | 0.67x | 16062 | 1.57x | 685MB |
| 12 | 1.00x | 16980 | 1.66x | 700MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mysql→postgres** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 26499 | - | 531MB |
| 2 | 0.17x | 33646 | 1.27x | 569MB |
| 4 | 0.33x | 28263 | 1.07x | 595MB |
| 8 | 0.67x | 40249 | 1.52x | 652MB |
| 12 | 1.00x | 40777 | 1.54x | 697MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mysql→redis** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 64458 | - | 567MB |
| 2 | 0.17x | 70155 | 1.09x | 534MB |
| 4 | 0.33x | 77817 | 1.21x | 617MB |
| 8 | 0.67x | 80684 | 1.25x | 618MB |
| 12 | 1.00x | 88817 | 1.38x | 694MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mysql→sqlite** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 15125 | - | 578MB |
| 2 | 0.17x | 16992 | 1.12x | 522MB |
| 4 | 0.33x | 18174 | 1.20x | 537MB |
| 8 | 0.67x | 17288 | 1.14x | 702MB |
| 12 | 1.00x | 17292 | 1.14x | 733MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**postgres→cockroachdb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 11627 | - | 606MB |
| 2 | 0.17x | 18511 | 1.59x | 545MB |
| 4 | 0.33x | 24052 | 2.07x | 648MB |
| 8 | 0.67x | 24111 | 2.07x | 657MB |
| 12 | 1.00x | 25933 | 2.23x | 699MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**postgres→mariadb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 22677 | - | 630MB |
| 2 | 0.17x | 26517 | 1.17x | 558MB |
| 4 | 0.33x | 29591 | 1.30x | 614MB |
| 8 | 0.67x | 30138 | 1.33x | 613MB |
| 12 | 1.00x | 30410 | 1.34x | 735MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**postgres→mongodb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 14756 | - | 540MB |
| 2 | 0.17x | 21921 | 1.49x | 569MB |
| 4 | 0.33x | 29197 | 1.98x | 615MB |
| 8 | 0.67x | 31491 | 2.13x | 629MB |
| 12 | 1.00x | 33334 | 2.26x | 743MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**postgres→mssql** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 1201.4 | - | 522MB |
| 2 | 0.17x | 1986.6 | 1.65x | 535MB |
| 4 | 0.33x | 3078.8 | 2.56x | 605MB |
| 8 | 0.67x | 4090.3 | 3.40x | 694MB |
| 12 | 1.00x | 4111.0 | 3.42x | 777MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**postgres→mysql** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 11062 | - | 577MB |
| 2 | 0.17x | 16829 | 1.52x | 555MB |
| 4 | 0.33x | 23299 | 2.11x | 608MB |
| 8 | 0.67x | 28189 | 2.55x | 746MB |
| 12 | 1.00x | 30061 | 2.72x | 737MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**postgres→postgres** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 25473 | - | 601MB |
| 2 | 0.17x | 33802 | 1.33x | 530MB |
| 4 | 0.33x | 43199 | 1.70x | 570MB |
| 8 | 0.67x | 49520 | 1.94x | 692MB |
| 12 | 1.00x | 45705 | 1.79x | 798MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**postgres→redis** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 45497 | - | 566MB |
| 2 | 0.17x | 50029 | 1.10x | 539MB |
| 4 | 0.33x | 59272 | 1.30x | 661MB |
| 8 | 0.67x | 62690 | 1.38x | 584MB |
| 12 | 1.00x | 61036 | 1.34x | 693MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**postgres→sqlite** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 15011 | - | 525MB |
| 2 | 0.17x | 17446 | 1.16x | 539MB |
| 4 | 0.33x | 18590 | 1.24x | 568MB |
| 8 | 0.67x | 18815 | 1.25x | 797MB |
| 12 | 1.00x | 18660 | 1.24x | 794MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**redis→cockroachdb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 2280.2 | - | 855MB |
| 2 | 0.17x | 3619.8 | 1.59x | 848MB |
| 4 | 0.33x | 5781.9 | 2.54x | 865MB |
| 8 | 0.67x | 7222.1 | 3.17x | 1.1GB |
| 12 | 1.00x | 7144.9 | 3.13x | 1010MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**redis→mariadb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 0.00 | - | 785MB |
| 2 | 0.17x | 0.00 | - | 795MB |
| 4 | 0.33x | 0.00 | - | 781MB |
| 8 | 0.67x | 0.00 | - | 847MB |
| 12 | 1.00x | 0.00 | - | 771MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**redis→mongodb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 11947 | - | 817MB |
| 2 | 0.17x | 12056 | 1.01x | 821MB |
| 4 | 0.33x | 12064 | 1.01x | 795MB |
| 8 | 0.67x | 12065 | 1.01x | 830MB |
| 12 | 1.00x | 12085 | 1.01x | 842MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**redis→mysql** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 0.00 | - | 821MB |
| 2 | 0.17x | 0.00 | - | 784MB |
| 4 | 0.33x | 0.00 | - | 800MB |
| 8 | 0.67x | 0.00 | - | 824MB |
| 12 | 1.00x | 0.00 | - | 839MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**redis→postgres** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 11498 | - | 779MB |
| 2 | 0.17x | 11685 | 1.02x | 836MB |
| 4 | 0.33x | 11774 | 1.02x | 831MB |
| 8 | 0.67x | 11844 | 1.03x | 850MB |
| 12 | 1.00x | 11822 | 1.03x | 848MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**redis→sqlite** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 11775 | - | 834MB |
| 2 | 0.17x | 12042 | 1.02x | 933MB |
| 4 | 0.33x | 11974 | 1.02x | 929MB |
| 8 | 0.67x | 11954 | 1.02x | 869MB |
| 12 | 1.00x | 12033 | 1.02x | 894MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**sqlite→cockroachdb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 14383 | - | 516MB |
| 2 | 0.17x | 13023 | 0.91x | 518MB |
| 4 | 0.33x | 19567 | 1.36x | 580MB |
| 8 | 0.67x | 21813 | 1.52x | 653MB |
| 12 | 1.00x | 23861 | 1.66x | 703MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**sqlite→mariadb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 24343 | - | 516MB |
| 2 | 0.17x | 30834 | 1.27x | 523MB |
| 4 | 0.33x | 36143 | 1.48x | 571MB |
| 8 | 0.67x | 37962 | 1.56x | 594MB |
| 12 | 1.00x | 43178 | 1.77x | 622MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**sqlite→mongodb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 23614 | - | 492MB |
| 2 | 0.17x | 36590 | 1.55x | 547MB |
| 4 | 0.33x | 53472 | 2.26x | 661MB |
| 8 | 0.67x | 50699 | 2.15x | 712MB |
| 12 | 1.00x | 59007 | 2.50x | 716MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**sqlite→mssql** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 1321.3 | - | 507MB |
| 2 | 0.17x | 2569.0 | 1.94x | 518MB |
| 4 | 0.33x | 4831.6 | 3.66x | 561MB |
| 8 | 0.67x | 7281.1 | 5.51x | 642MB |
| 12 | 1.00x | 8651.6 | 6.55x | 777MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**sqlite→mysql** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 9121.6 | - | 506MB |
| 2 | 0.17x | 15401 | 1.69x | 522MB |
| 4 | 0.33x | 17663 | 1.94x | 599MB |
| 8 | 0.67x | 14957 | 1.64x | 714MB |
| 12 | 1.00x | 9578.5 | 1.05x | 647MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**sqlite→postgres** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 26766 | - | 542MB |
| 2 | 0.17x | 34572 | 1.29x | 531MB |
| 4 | 0.33x | 32309 | 1.21x | 597MB |
| 8 | 0.67x | 39746 | 1.48x | 662MB |
| 12 | 1.00x | 41827 | 1.56x | 660MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**sqlite→redis** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 64744 | - | 530MB |
| 2 | 0.17x | 68901 | 1.06x | 548MB |
| 4 | 0.33x | 72841 | 1.13x | 634MB |
| 8 | 0.67x | 79964 | 1.24x | 634MB |
| 12 | 1.00x | 88533 | 1.37x | 704MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**sqlite→sqlite** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 17254 | - | 552MB |
| 2 | 0.17x | 16173 | 0.94x | 567MB |
| 4 | 0.33x | 19761 | 1.15x | 596MB |
| 8 | 0.67x | 17083 | 0.99x | 608MB |
| 12 | 1.00x | 19646 | 1.14x | 636MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

### Phase Breakdown

| Scenario | Size | Schema | Transfer | Verify |
|----------|------|-------:|---------:|-------:|
| sqlite→sqlite-large-b1000-w1-vfalse | large | 182ms | 0s | 0s |
| sqlite→sqlite-large-b1000-w2-vfalse | large | 202ms | 0s | 0s |
| sqlite→sqlite-large-b1000-w4-vfalse | large | 204ms | 0s | 0s |
| sqlite→sqlite-large-b1000-w8-vfalse | large | 193ms | 0s | 0s |
| sqlite→sqlite-large-b1000-w12-vfalse | large | 193ms | 0s | 0s |
| sqlite→postgres-large-b1000-w1-vfalse | large | 1ms | 0s | 0s |
| sqlite→postgres-large-b1000-w2-vfalse | large | 1ms | 0s | 0s |
| sqlite→postgres-large-b1000-w4-vfalse | large | 1ms | 0s | 0s |
| sqlite→postgres-large-b1000-w8-vfalse | large | 1ms | 0s | 0s |
| sqlite→postgres-large-b1000-w12-vfalse | large | 1ms | 0s | 0s |
| sqlite→mysql-large-b1000-w1-vfalse | large | 928ms | 0s | 0s |
| sqlite→mysql-large-b1000-w2-vfalse | large | 939ms | 0s | 0s |
| sqlite→mysql-large-b1000-w4-vfalse | large | 773ms | 0s | 0s |
| sqlite→mysql-large-b1000-w8-vfalse | large | 751ms | 0s | 0s |
| sqlite→mysql-large-b1000-w12-vfalse | large | 808ms | 0s | 0s |
| sqlite→mariadb-large-b1000-w1-vfalse | large | 261ms | 0s | 0s |
| sqlite→mariadb-large-b1000-w2-vfalse | large | 296ms | 0s | 0s |
| sqlite→mariadb-large-b1000-w4-vfalse | large | 251ms | 0s | 0s |
| sqlite→mariadb-large-b1000-w8-vfalse | large | 298ms | 0s | 0s |
| sqlite→mariadb-large-b1000-w12-vfalse | large | 232ms | 0s | 0s |
| sqlite→cockroachdb-large-b1000-w1-vfalse | large | 122ms | 0s | 0s |
| sqlite→cockroachdb-large-b1000-w2-vfalse | large | 54ms | 0s | 0s |
| sqlite→cockroachdb-large-b1000-w4-vfalse | large | 55ms | 0s | 0s |
| sqlite→cockroachdb-large-b1000-w8-vfalse | large | 56ms | 0s | 0s |
| sqlite→cockroachdb-large-b1000-w12-vfalse | large | 53ms | 0s | 0s |
| sqlite→mssql-large-b1000-w1-vfalse | large | 103ms | 0s | 0s |
| sqlite→mssql-large-b1000-w2-vfalse | large | 99ms | 0s | 0s |
| sqlite→mssql-large-b1000-w4-vfalse | large | 100ms | 0s | 0s |
| sqlite→mssql-large-b1000-w8-vfalse | large | 94ms | 0s | 0s |
| sqlite→mssql-large-b1000-w12-vfalse | large | 98ms | 0s | 0s |
| sqlite→mongodb-large-b1000-w1-vfalse | large | 539ms | 0s | 0s |
| sqlite→mongodb-large-b1000-w2-vfalse | large | 501ms | 0s | 0s |
| sqlite→mongodb-large-b1000-w4-vfalse | large | 538ms | 0s | 0s |
| sqlite→mongodb-large-b1000-w8-vfalse | large | 555ms | 0s | 0s |
| sqlite→mongodb-large-b1000-w12-vfalse | large | 571ms | 0s | 0s |
| sqlite→redis-large-b1000-w1-vfalse | large | 0s | 0s | 0s |
| sqlite→redis-large-b1000-w2-vfalse | large | 0s | 0s | 0s |
| sqlite→redis-large-b1000-w4-vfalse | large | 0s | 0s | 0s |
| sqlite→redis-large-b1000-w8-vfalse | large | 0s | 0s | 0s |
| sqlite→redis-large-b1000-w12-vfalse | large | 0s | 0s | 0s |
| postgres→sqlite-large-b1000-w1-vfalse | large | 185ms | 0s | 0s |
| postgres→sqlite-large-b1000-w2-vfalse | large | 364ms | 0s | 0s |
| postgres→sqlite-large-b1000-w4-vfalse | large | 382ms | 0s | 0s |
| postgres→sqlite-large-b1000-w8-vfalse | large | 216ms | 0s | 0s |
| postgres→sqlite-large-b1000-w12-vfalse | large | 190ms | 0s | 0s |
| postgres→postgres-large-b1000-w1-vfalse | large | 4ms | 0s | 0s |
| postgres→postgres-large-b1000-w2-vfalse | large | 4ms | 0s | 0s |
| postgres→postgres-large-b1000-w4-vfalse | large | 4ms | 0s | 0s |
| postgres→postgres-large-b1000-w8-vfalse | large | 4ms | 0s | 0s |
| postgres→postgres-large-b1000-w12-vfalse | large | 3ms | 0s | 0s |
| postgres→mysql-large-b1000-w1-vfalse | large | 972ms | 0s | 0s |
| postgres→mysql-large-b1000-w2-vfalse | large | 756ms | 0s | 0s |
| postgres→mysql-large-b1000-w4-vfalse | large | 770ms | 0s | 0s |
| postgres→mysql-large-b1000-w8-vfalse | large | 1.155s | 0s | 0s |
| postgres→mysql-large-b1000-w12-vfalse | large | 1.002s | 0s | 0s |
| postgres→mariadb-large-b1000-w1-vfalse | large | 246ms | 0s | 0s |
| postgres→mariadb-large-b1000-w2-vfalse | large | 278ms | 0s | 0s |
| postgres→mariadb-large-b1000-w4-vfalse | large | 297ms | 0s | 0s |
| postgres→mariadb-large-b1000-w8-vfalse | large | 333ms | 0s | 0s |
| postgres→mariadb-large-b1000-w12-vfalse | large | 240ms | 0s | 0s |
| postgres→cockroachdb-large-b1000-w1-vfalse | large | 57ms | 0s | 0s |
| postgres→cockroachdb-large-b1000-w2-vfalse | large | 57ms | 0s | 0s |
| postgres→cockroachdb-large-b1000-w4-vfalse | large | 57ms | 0s | 0s |
| postgres→cockroachdb-large-b1000-w8-vfalse | large | 56ms | 0s | 0s |
| postgres→cockroachdb-large-b1000-w12-vfalse | large | 57ms | 0s | 0s |
| postgres→mssql-large-b1000-w1-vfalse | large | 104ms | 0s | 0s |
| postgres→mssql-large-b1000-w2-vfalse | large | 99ms | 0s | 0s |
| postgres→mssql-large-b1000-w4-vfalse | large | 98ms | 0s | 0s |
| postgres→mssql-large-b1000-w8-vfalse | large | 105ms | 0s | 0s |
| postgres→mssql-large-b1000-w12-vfalse | large | 101ms | 0s | 0s |
| postgres→mongodb-large-b1000-w1-vfalse | large | 208ms | 0s | 0s |
| postgres→mongodb-large-b1000-w2-vfalse | large | 247ms | 0s | 0s |
| postgres→mongodb-large-b1000-w4-vfalse | large | 213ms | 0s | 0s |
| postgres→mongodb-large-b1000-w8-vfalse | large | 232ms | 0s | 0s |
| postgres→mongodb-large-b1000-w12-vfalse | large | 224ms | 0s | 0s |
| postgres→redis-large-b1000-w1-vfalse | large | 0s | 0s | 0s |
| postgres→redis-large-b1000-w2-vfalse | large | 0s | 0s | 0s |
| postgres→redis-large-b1000-w4-vfalse | large | 0s | 0s | 0s |
| postgres→redis-large-b1000-w8-vfalse | large | 0s | 0s | 0s |
| postgres→redis-large-b1000-w12-vfalse | large | 0s | 0s | 0s |
| mysql→sqlite-large-b1000-w1-vfalse | large | 220ms | 0s | 0s |
| mysql→sqlite-large-b1000-w2-vfalse | large | 226ms | 0s | 0s |
| mysql→sqlite-large-b1000-w4-vfalse | large | 195ms | 0s | 0s |
| mysql→sqlite-large-b1000-w8-vfalse | large | 185ms | 0s | 0s |
| mysql→sqlite-large-b1000-w12-vfalse | large | 173ms | 0s | 0s |
| mysql→postgres-large-b1000-w1-vfalse | large | 5ms | 0s | 0s |
| mysql→postgres-large-b1000-w2-vfalse | large | 4ms | 0s | 0s |
| mysql→postgres-large-b1000-w4-vfalse | large | 4ms | 0s | 0s |
| mysql→postgres-large-b1000-w8-vfalse | large | 4ms | 0s | 0s |
| mysql→postgres-large-b1000-w12-vfalse | large | 4ms | 0s | 0s |
| mysql→mysql-large-b1000-w1-vfalse | large | 1.089s | 0s | 0s |
| mysql→mysql-large-b1000-w2-vfalse | large | 1.137s | 0s | 0s |
| mysql→mysql-large-b1000-w4-vfalse | large | 929ms | 0s | 0s |
| mysql→mysql-large-b1000-w8-vfalse | large | 880ms | 0s | 0s |
| mysql→mysql-large-b1000-w12-vfalse | large | 881ms | 0s | 0s |
| mysql→mariadb-large-b1000-w1-vfalse | large | 312ms | 0s | 0s |
| mysql→mariadb-large-b1000-w2-vfalse | large | 232ms | 0s | 0s |
| mysql→mariadb-large-b1000-w4-vfalse | large | 246ms | 0s | 0s |
| mysql→mariadb-large-b1000-w8-vfalse | large | 286ms | 0s | 0s |
| mysql→mariadb-large-b1000-w12-vfalse | large | 233ms | 0s | 0s |
| mysql→cockroachdb-large-b1000-w1-vfalse | large | 61ms | 0s | 0s |
| mysql→cockroachdb-large-b1000-w2-vfalse | large | 58ms | 0s | 0s |
| mysql→cockroachdb-large-b1000-w4-vfalse | large | 59ms | 0s | 0s |
| mysql→cockroachdb-large-b1000-w8-vfalse | large | 57ms | 0s | 0s |
| mysql→cockroachdb-large-b1000-w12-vfalse | large | 58ms | 0s | 0s |
| mysql→mssql-large-b1000-w1-vfalse | large | 113ms | 0s | 0s |
| mysql→mssql-large-b1000-w2-vfalse | large | 102ms | 0s | 0s |
| mysql→mssql-large-b1000-w4-vfalse | large | 104ms | 0s | 0s |
| mysql→mssql-large-b1000-w8-vfalse | large | 106ms | 0s | 0s |
| mysql→mssql-large-b1000-w12-vfalse | large | 107ms | 0s | 0s |
| mysql→mongodb-large-b1000-w1-vfalse | large | 580ms | 0s | 0s |
| mysql→mongodb-large-b1000-w2-vfalse | large | 519ms | 0s | 0s |
| mysql→mongodb-large-b1000-w4-vfalse | large | 529ms | 0s | 0s |
| mysql→mongodb-large-b1000-w8-vfalse | large | 550ms | 0s | 0s |
| mysql→mongodb-large-b1000-w12-vfalse | large | 626ms | 0s | 0s |
| mysql→redis-large-b1000-w1-vfalse | large | 0s | 0s | 0s |
| mysql→redis-large-b1000-w2-vfalse | large | 0s | 0s | 0s |
| mysql→redis-large-b1000-w4-vfalse | large | 0s | 0s | 0s |
| mysql→redis-large-b1000-w8-vfalse | large | 0s | 0s | 0s |
| mysql→redis-large-b1000-w12-vfalse | large | 0s | 0s | 0s |
| mariadb→sqlite-large-b1000-w1-vfalse | large | 228ms | 0s | 0s |
| mariadb→sqlite-large-b1000-w2-vfalse | large | 172ms | 0s | 0s |
| mariadb→sqlite-large-b1000-w4-vfalse | large | 191ms | 0s | 0s |
| mariadb→sqlite-large-b1000-w8-vfalse | large | 196ms | 0s | 0s |
| mariadb→sqlite-large-b1000-w12-vfalse | large | 201ms | 0s | 0s |
| mariadb→postgres-large-b1000-w1-vfalse | large | 2ms | 0s | 0s |
| mariadb→postgres-large-b1000-w2-vfalse | large | 2ms | 0s | 0s |
| mariadb→postgres-large-b1000-w4-vfalse | large | 2ms | 0s | 0s |
| mariadb→postgres-large-b1000-w8-vfalse | large | 2ms | 0s | 0s |
| mariadb→postgres-large-b1000-w12-vfalse | large | 2ms | 0s | 0s |
| mariadb→mysql-large-b1000-w1-vfalse | large | 862ms | 0s | 0s |
| mariadb→mysql-large-b1000-w2-vfalse | large | 973ms | 0s | 0s |
| mariadb→mysql-large-b1000-w4-vfalse | large | 981ms | 0s | 0s |
| mariadb→mysql-large-b1000-w8-vfalse | large | 799ms | 0s | 0s |
| mariadb→mysql-large-b1000-w12-vfalse | large | 905ms | 0s | 0s |
| mariadb→mariadb-large-b1000-w1-vfalse | large | 247ms | 0s | 0s |
| mariadb→mariadb-large-b1000-w2-vfalse | large | 272ms | 0s | 0s |
| mariadb→mariadb-large-b1000-w4-vfalse | large | 245ms | 0s | 0s |
| mariadb→mariadb-large-b1000-w8-vfalse | large | 390ms | 0s | 0s |
| mariadb→mariadb-large-b1000-w12-vfalse | large | 279ms | 0s | 0s |
| mariadb→cockroachdb-large-b1000-w1-vfalse | large | 57ms | 0s | 0s |
| mariadb→cockroachdb-large-b1000-w2-vfalse | large | 55ms | 0s | 0s |
| mariadb→cockroachdb-large-b1000-w4-vfalse | large | 57ms | 0s | 0s |
| mariadb→cockroachdb-large-b1000-w8-vfalse | large | 55ms | 0s | 0s |
| mariadb→cockroachdb-large-b1000-w12-vfalse | large | 55ms | 0s | 0s |
| mariadb→mssql-large-b1000-w1-vfalse | large | 190ms | 0s | 0s |
| mariadb→mssql-large-b1000-w2-vfalse | large | 99ms | 0s | 0s |
| mariadb→mssql-large-b1000-w4-vfalse | large | 103ms | 0s | 0s |
| mariadb→mssql-large-b1000-w8-vfalse | large | 95ms | 0s | 0s |
| mariadb→mssql-large-b1000-w12-vfalse | large | 96ms | 0s | 0s |
| mariadb→mongodb-large-b1000-w1-vfalse | large | 1ms | 0s | 0s |
| mariadb→mongodb-large-b1000-w2-vfalse | large | 1ms | 0s | 0s |
| mariadb→mongodb-large-b1000-w4-vfalse | large | 1ms | 0s | 0s |
| mariadb→mongodb-large-b1000-w8-vfalse | large | 1ms | 0s | 0s |
| mariadb→mongodb-large-b1000-w12-vfalse | large | 1ms | 0s | 0s |
| mariadb→redis-large-b1000-w1-vfalse | large | 0s | 0s | 0s |
| mariadb→redis-large-b1000-w2-vfalse | large | 0s | 0s | 0s |
| mariadb→redis-large-b1000-w4-vfalse | large | 0s | 0s | 0s |
| mariadb→redis-large-b1000-w8-vfalse | large | 0s | 0s | 0s |
| mariadb→redis-large-b1000-w12-vfalse | large | 0s | 0s | 0s |
| cockroachdb→sqlite-large-b1000-w1-vfalse | large | 339ms | 0s | 0s |
| cockroachdb→sqlite-large-b1000-w2-vfalse | large | 334ms | 0s | 0s |
| cockroachdb→sqlite-large-b1000-w4-vfalse | large | 371ms | 0s | 0s |
| cockroachdb→sqlite-large-b1000-w8-vfalse | large | 341ms | 0s | 0s |
| cockroachdb→sqlite-large-b1000-w12-vfalse | large | 379ms | 0s | 0s |
| cockroachdb→postgres-large-b1000-w1-vfalse | large | 137ms | 0s | 0s |
| cockroachdb→postgres-large-b1000-w2-vfalse | large | 139ms | 0s | 0s |
| cockroachdb→postgres-large-b1000-w4-vfalse | large | 132ms | 0s | 0s |
| cockroachdb→postgres-large-b1000-w8-vfalse | large | 130ms | 0s | 0s |
| cockroachdb→postgres-large-b1000-w12-vfalse | large | 135ms | 0s | 0s |
| cockroachdb→mysql-large-b1000-w1-vfalse | large | 1.189s | 0s | 0s |
| cockroachdb→mysql-large-b1000-w2-vfalse | large | 1.105s | 0s | 0s |
| cockroachdb→mysql-large-b1000-w4-vfalse | large | 942ms | 0s | 0s |
| cockroachdb→mysql-large-b1000-w8-vfalse | large | 1.206s | 0s | 0s |
| cockroachdb→mysql-large-b1000-w12-vfalse | large | 1.044s | 0s | 0s |
| cockroachdb→mariadb-large-b1000-w1-vfalse | large | 401ms | 0s | 0s |
| cockroachdb→mariadb-large-b1000-w2-vfalse | large | 365ms | 0s | 0s |
| cockroachdb→mariadb-large-b1000-w4-vfalse | large | 519ms | 0s | 0s |
| cockroachdb→mariadb-large-b1000-w8-vfalse | large | 551ms | 0s | 0s |
| cockroachdb→mariadb-large-b1000-w12-vfalse | large | 386ms | 0s | 0s |
| cockroachdb→cockroachdb-large-b1000-w1-vfalse | large | 190ms | 0s | 0s |
| cockroachdb→cockroachdb-large-b1000-w2-vfalse | large | 203ms | 0s | 0s |
| cockroachdb→cockroachdb-large-b1000-w4-vfalse | large | 192ms | 0s | 0s |
| cockroachdb→cockroachdb-large-b1000-w8-vfalse | large | 214ms | 0s | 0s |
| cockroachdb→cockroachdb-large-b1000-w12-vfalse | large | 201ms | 0s | 0s |
| cockroachdb→mongodb-large-b1000-w1-vfalse | large | 380ms | 0s | 0s |
| cockroachdb→mongodb-large-b1000-w2-vfalse | large | 363ms | 0s | 0s |
| cockroachdb→mongodb-large-b1000-w4-vfalse | large | 374ms | 0s | 0s |
| cockroachdb→mongodb-large-b1000-w8-vfalse | large | 360ms | 0s | 0s |
| cockroachdb→mongodb-large-b1000-w12-vfalse | large | 367ms | 0s | 0s |
| cockroachdb→redis-large-b1000-w1-vfalse | large | 0s | 0s | 0s |
| cockroachdb→redis-large-b1000-w2-vfalse | large | 0s | 0s | 0s |
| cockroachdb→redis-large-b1000-w4-vfalse | large | 0s | 0s | 0s |
| cockroachdb→redis-large-b1000-w8-vfalse | large | 0s | 0s | 0s |
| mssql→sqlite-large-b1000-w1-vfalse | large | 289ms | 0s | 0s |
| mssql→sqlite-large-b1000-w2-vfalse | large | 202ms | 0s | 0s |
| mssql→sqlite-large-b1000-w4-vfalse | large | 199ms | 0s | 0s |
| mssql→sqlite-large-b1000-w8-vfalse | large | 228ms | 0s | 0s |
| mssql→sqlite-large-b1000-w12-vfalse | large | 368ms | 0s | 0s |
| mssql→postgres-large-b1000-w1-vfalse | large | 6ms | 0s | 0s |
| mssql→postgres-large-b1000-w2-vfalse | large | 6ms | 0s | 0s |
| mssql→postgres-large-b1000-w4-vfalse | large | 6ms | 0s | 0s |
| mssql→postgres-large-b1000-w8-vfalse | large | 5ms | 0s | 0s |
| mssql→postgres-large-b1000-w12-vfalse | large | 6ms | 0s | 0s |
| mssql→mysql-large-b1000-w1-vfalse | large | 1.047s | 0s | 0s |
| mssql→mysql-large-b1000-w2-vfalse | large | 889ms | 0s | 0s |
| mssql→mysql-large-b1000-w4-vfalse | large | 907ms | 0s | 0s |
| mssql→mysql-large-b1000-w8-vfalse | large | 802ms | 0s | 0s |
| mssql→mysql-large-b1000-w12-vfalse | large | 826ms | 0s | 0s |
| mssql→mariadb-large-b1000-w1-vfalse | large | 265ms | 0s | 0s |
| mssql→mariadb-large-b1000-w2-vfalse | large | 310ms | 0s | 0s |
| mssql→mariadb-large-b1000-w4-vfalse | large | 345ms | 0s | 0s |
| mssql→mariadb-large-b1000-w8-vfalse | large | 279ms | 0s | 0s |
| mssql→mariadb-large-b1000-w12-vfalse | large | 317ms | 0s | 0s |
| mssql→cockroachdb-large-b1000-w1-vfalse | large | 62ms | 0s | 0s |
| mssql→cockroachdb-large-b1000-w2-vfalse | large | 60ms | 0s | 0s |
| mssql→cockroachdb-large-b1000-w4-vfalse | large | 60ms | 0s | 0s |
| mssql→cockroachdb-large-b1000-w8-vfalse | large | 62ms | 0s | 0s |
| mssql→cockroachdb-large-b1000-w12-vfalse | large | 59ms | 0s | 0s |
| mssql→mssql-large-b1000-w1-vfalse | large | 150ms | 0s | 0s |
| mssql→mssql-large-b1000-w2-vfalse | large | 96ms | 0s | 0s |
| mssql→mssql-large-b1000-w4-vfalse | large | 111ms | 0s | 0s |
| mssql→mssql-large-b1000-w8-vfalse | large | 96ms | 0s | 0s |
| mssql→mssql-large-b1000-w12-vfalse | large | 95ms | 0s | 0s |
| mssql→mongodb-large-b1000-w1-vfalse | large | 618ms | 0s | 0s |
| mssql→mongodb-large-b1000-w2-vfalse | large | 537ms | 0s | 0s |
| mssql→mongodb-large-b1000-w4-vfalse | large | 1.843s | 0s | 0s |
| mssql→mongodb-large-b1000-w8-vfalse | large | 530ms | 0s | 0s |
| mssql→mongodb-large-b1000-w12-vfalse | large | 671ms | 0s | 0s |
| mongodb→sqlite-large-b1000-w1-vfalse | large | 0s | 0s | 0s |
| mongodb→sqlite-large-b1000-w2-vfalse | large | 0s | 0s | 0s |
| mongodb→sqlite-large-b1000-w4-vfalse | large | 0s | 0s | 0s |
| mongodb→sqlite-large-b1000-w8-vfalse | large | 0s | 0s | 0s |
| mongodb→sqlite-large-b1000-w12-vfalse | large | 0s | 0s | 0s |
| mongodb→postgres-large-b1000-w1-vfalse | large | 0s | 0s | 0s |
| mongodb→postgres-large-b1000-w2-vfalse | large | 0s | 0s | 0s |
| mongodb→postgres-large-b1000-w4-vfalse | large | 0s | 0s | 0s |
| mongodb→postgres-large-b1000-w8-vfalse | large | 0s | 0s | 0s |
| mongodb→postgres-large-b1000-w12-vfalse | large | 0s | 0s | 0s |
| mongodb→mysql-large-b1000-w1-vfalse | large | 0s | 0s | 0s |
| mongodb→mysql-large-b1000-w2-vfalse | large | 1ms | 0s | 0s |
| mongodb→mysql-large-b1000-w4-vfalse | large | 0s | 0s | 0s |
| mongodb→mysql-large-b1000-w8-vfalse | large | 0s | 0s | 0s |
| mongodb→mysql-large-b1000-w12-vfalse | large | 0s | 0s | 0s |
| mongodb→mariadb-large-b1000-w1-vfalse | large | 1ms | 0s | 0s |
| mongodb→mariadb-large-b1000-w2-vfalse | large | 0s | 0s | 0s |
| mongodb→mariadb-large-b1000-w4-vfalse | large | 0s | 0s | 0s |
| mongodb→mariadb-large-b1000-w8-vfalse | large | 0s | 0s | 0s |
| mongodb→mariadb-large-b1000-w12-vfalse | large | 0s | 0s | 0s |
| mongodb→cockroachdb-large-b1000-w1-vfalse | large | 0s | 0s | 0s |
| mongodb→cockroachdb-large-b1000-w2-vfalse | large | 1ms | 0s | 0s |
| mongodb→cockroachdb-large-b1000-w4-vfalse | large | 0s | 0s | 0s |
| mongodb→cockroachdb-large-b1000-w8-vfalse | large | 0s | 0s | 0s |
| mongodb→cockroachdb-large-b1000-w12-vfalse | large | 1ms | 0s | 0s |
| mongodb→mssql-large-b1000-w1-vfalse | large | 0s | 0s | 0s |
| mongodb→mssql-large-b1000-w2-vfalse | large | 0s | 0s | 0s |
| mongodb→mssql-large-b1000-w4-vfalse | large | 0s | 0s | 0s |
| mongodb→mssql-large-b1000-w8-vfalse | large | 0s | 0s | 0s |
| mongodb→mssql-large-b1000-w12-vfalse | large | 0s | 0s | 0s |
| mongodb→mongodb-large-b1000-w1-vfalse | large | 0s | 0s | 0s |
| mongodb→mongodb-large-b1000-w2-vfalse | large | 0s | 0s | 0s |
| mongodb→mongodb-large-b1000-w4-vfalse | large | 0s | 0s | 0s |
| mongodb→mongodb-large-b1000-w8-vfalse | large | 0s | 0s | 0s |
| mongodb→mongodb-large-b1000-w12-vfalse | large | 0s | 0s | 0s |
| redis→sqlite-large-b1000-w1-vfalse | large | 0s | 0s | 0s |
| redis→sqlite-large-b1000-w2-vfalse | large | 0s | 0s | 0s |
| redis→sqlite-large-b1000-w4-vfalse | large | 0s | 0s | 0s |
| redis→sqlite-large-b1000-w8-vfalse | large | 0s | 0s | 0s |
| redis→sqlite-large-b1000-w12-vfalse | large | 0s | 0s | 0s |
| redis→postgres-large-b1000-w1-vfalse | large | 0s | 0s | 0s |
| redis→postgres-large-b1000-w2-vfalse | large | 0s | 0s | 0s |
| redis→postgres-large-b1000-w4-vfalse | large | 0s | 0s | 0s |
| redis→postgres-large-b1000-w8-vfalse | large | 0s | 0s | 0s |
| redis→postgres-large-b1000-w12-vfalse | large | 0s | 0s | 0s |
| redis→mysql-large-b1000-w1-vfalse | large | 0s | 0s | 0s |
| redis→mysql-large-b1000-w2-vfalse | large | 0s | 0s | 0s |
| redis→mysql-large-b1000-w4-vfalse | large | 0s | 0s | 0s |
| redis→mysql-large-b1000-w8-vfalse | large | 0s | 0s | 0s |
| redis→mysql-large-b1000-w12-vfalse | large | 0s | 0s | 0s |
| redis→mariadb-large-b1000-w1-vfalse | large | 0s | 0s | 0s |
| redis→mariadb-large-b1000-w2-vfalse | large | 0s | 0s | 0s |
| redis→mariadb-large-b1000-w4-vfalse | large | 0s | 0s | 0s |
| redis→mariadb-large-b1000-w8-vfalse | large | 0s | 0s | 0s |
| redis→mariadb-large-b1000-w12-vfalse | large | 0s | 0s | 0s |
| redis→cockroachdb-large-b1000-w1-vfalse | large | 0s | 0s | 0s |
| redis→cockroachdb-large-b1000-w2-vfalse | large | 0s | 0s | 0s |
| redis→cockroachdb-large-b1000-w4-vfalse | large | 0s | 0s | 0s |
| redis→cockroachdb-large-b1000-w8-vfalse | large | 0s | 0s | 0s |
| redis→cockroachdb-large-b1000-w12-vfalse | large | 0s | 0s | 0s |
| redis→mongodb-large-b1000-w1-vfalse | large | 0s | 0s | 0s |
| redis→mongodb-large-b1000-w2-vfalse | large | 0s | 0s | 0s |
| redis→mongodb-large-b1000-w4-vfalse | large | 0s | 0s | 0s |
| redis→mongodb-large-b1000-w8-vfalse | large | 0s | 0s | 0s |
| redis→mongodb-large-b1000-w12-vfalse | large | 0s | 0s | 0s |

### Interpreting Results

- **CPU impact**: Workers beyond 12 cores will compete for CPU time.
  Diminishing returns are expected when workers >> cores.
- **RAM impact**: Peak RAM should stay well below 31GB total system memory.
  If Peak RAM approaches system total, expect swapping and degraded performance.
- **Batch size**: Larger batches reduce per-row overhead but increase memory usage.
  The optimal size depends on row width and available memory.
- **Verification**: Adds a full pass over destination data; overhead is proportional
  to dataset size and verification depth.
- **Disk I/O**: SQLite uses file-based I/O. SSD results will differ significantly from HDD.
