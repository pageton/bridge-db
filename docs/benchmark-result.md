# Bridge-DB Benchmark Report

Generated: 2026-04-16T00:17:46Z


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
| sqlite→sqlite | small | 10K | 2MB | 1000 | 1 | false | 183ms | 54649 | 32.16 | 52MB |
| sqlite→sqlite | small | 10K | 2MB | 1000 | 4 | false | 163ms | 61312 | 36.08 | 86MB |
| sqlite→postgres | small | 10K | 2MB | 1000 | 1 | false | 231ms | 43246 | 25.45 | 52MB |
| sqlite→postgres | small | 10K | 2MB | 1000 | 4 | false | 230ms | 43530 | 25.62 | 50MB |
| sqlite→mysql | small | 10K | 2MB | 1000 | 1 | false | 571ms | 17501 | 10.30 | 57MB |
| sqlite→mysql | small | 10K | 2MB | 1000 | 4 | false | 391ms | 25580 | 15.05 | 82MB |
| sqlite→mariadb | small | 10K | 2MB | 1000 | 1 | false | 334ms | 29932 | 17.61 | 59MB |
| sqlite→mariadb | small | 10K | 2MB | 1000 | 4 | false | 241ms | 41427 | 24.38 | 72MB |
| sqlite→mongodb | small | 10K | 2MB | 1000 | 1 | false | 418ms | 23941 | 18.36 | 57MB |
| sqlite→mongodb | small | 10K | 2MB | 1000 | 4 | false | 171ms | 58328 | 44.73 | 79MB |
| sqlite→redis | small | 10K | 2MB | 1000 | 1 | false | 105ms | 95373 | 72.33 | 53MB |
| sqlite→redis | small | 10K | 2MB | 1000 | 4 | false | 100ms | 100348 | 76.10 | 50MB |
| postgres→sqlite | small | 10K | 2MB | 1000 | 1 | false | 1.667s | 0.00 | 0.00 | 44MB |
| postgres→sqlite | small | 10K | 2MB | 1000 | 4 | false | 1.614s | 0.00 | 0.00 | 38MB |
| postgres→postgres | small | 10K | 2MB | 1000 | 1 | false | 1.45s | 0.00 | 0.00 | 39MB |
| postgres→postgres | small | 10K | 2MB | 1000 | 4 | false | 1.464s | 0.00 | 0.00 | 37MB |
| postgres→mysql | small | 10K | 2MB | 1000 | 1 | false | 1.563s | 0.00 | 0.00 | 45MB |
| postgres→mysql | small | 10K | 2MB | 1000 | 4 | false | 1.531s | 0.00 | 0.00 | 42MB |
| postgres→mariadb | small | 10K | 2MB | 1000 | 1 | false | 1.421s | 0.00 | 0.00 | 46MB |
| postgres→mariadb | small | 10K | 2MB | 1000 | 4 | false | 1.39s | 0.00 | 0.00 | 47MB |
| postgres→mongodb | small | 10K | 2MB | 1000 | 1 | false | 1.368s | 0.00 | 0.00 | 45MB |
| postgres→mongodb | small | 10K | 2MB | 1000 | 4 | false | 1.365s | 0.00 | 0.00 | 42MB |
| postgres→redis | small | 10K | 2MB | 1000 | 1 | false | 1.337s | 0.00 | 0.00 | 39MB |
| postgres→redis | small | 10K | 2MB | 1000 | 4 | false | 1.34s | 0.00 | 0.00 | 39MB |
| mysql→sqlite | small | 10K | 2MB | 1000 | 1 | false | 252ms | 39706 | 23.38 | 56MB |
| mysql→sqlite | small | 10K | 2MB | 1000 | 4 | false | 223ms | 44930 | 26.45 | 57MB |
| mysql→postgres | small | 10K | 2MB | 1000 | 1 | false | 251ms | 39913 | 23.50 | 53MB |
| mysql→postgres | small | 10K | 2MB | 1000 | 4 | false | 240ms | 41654 | 24.53 | 52MB |
| mysql→mysql | small | 10K | 2MB | 1000 | 1 | false | 534ms | 18742 | 11.04 | 59MB |
| mysql→mysql | small | 10K | 2MB | 1000 | 4 | false | 291ms | 34423 | 20.27 | 87MB |
| mysql→mariadb | small | 10K | 2MB | 1000 | 1 | false | 364ms | 27441 | 16.16 | 59MB |
| mysql→mariadb | small | 10K | 2MB | 1000 | 4 | false | 243ms | 41085 | 24.19 | 74MB |
| mysql→mongodb | small | 10K | 2MB | 1000 | 1 | false | 377ms | 26536 | 20.04 | 56MB |
| mysql→mongodb | small | 10K | 2MB | 1000 | 4 | false | 180ms | 55565 | 41.97 | 75MB |
| mysql→redis | small | 10K | 2MB | 1000 | 1 | false | 105ms | 95062 | 69.89 | 46MB |
| mysql→redis | small | 10K | 2MB | 1000 | 4 | false | 98ms | 101939 | 74.94 | 46MB |
| mariadb→sqlite | small | 10K | 2MB | 1000 | 1 | false | 237ms | 42143 | 25.30 | 56MB |
| mariadb→sqlite | small | 10K | 2MB | 1000 | 4 | false | 232ms | 43183 | 25.92 | 59MB |
| mariadb→postgres | small | 10K | 2MB | 1000 | 1 | false | 244ms | 41015 | 24.62 | 53MB |
| mariadb→postgres | small | 10K | 2MB | 1000 | 4 | false | 238ms | 42070 | 25.25 | 53MB |
| mariadb→mysql | small | 10K | 2MB | 1000 | 1 | false | 537ms | 18615 | 11.17 | 57MB |
| mariadb→mysql | small | 10K | 2MB | 1000 | 4 | false | 390ms | 25674 | 15.41 | 84MB |
| mariadb→mariadb | small | 10K | 2MB | 1000 | 1 | false | 327ms | 30614 | 18.38 | 54MB |
| mariadb→mariadb | small | 10K | 2MB | 1000 | 4 | false | 239ms | 41844 | 25.12 | 86MB |
| mariadb→mongodb | small | 10K | 2MB | 1000 | 1 | false | 340ms | 29423 | 22.22 | 56MB |
| mariadb→mongodb | small | 10K | 2MB | 1000 | 4 | false | 258ms | 38690 | 29.22 | 58MB |
| mariadb→redis | small | 10K | 2MB | 1000 | 1 | false | 238ms | 42047 | 30.91 | 47MB |
| mariadb→redis | small | 10K | 2MB | 1000 | 4 | false | 237ms | 42112 | 30.96 | 48MB |
| mongodb→sqlite | small | 10K | 2MB | 1000 | 1 | false | 204ms | 48976 | 30.11 | 58MB |
| mongodb→sqlite | small | 10K | 2MB | 1000 | 4 | false | 175ms | 57186 | 35.15 | 72MB |
| mongodb→postgres | small | 10K | 2MB | 1000 | 1 | false | 209ms | 47931 | 30.29 | 58MB |
| mongodb→postgres | small | 10K | 2MB | 1000 | 4 | false | 167ms | 59863 | 37.83 | 57MB |
| mongodb→mysql | small | 10K | 2MB | 1000 | 1 | false | 565ms | 17693 | 10.88 | 61MB |
| mongodb→mysql | small | 10K | 2MB | 1000 | 4 | false | 332ms | 30106 | 18.51 | 90MB |
| mongodb→mariadb | small | 10K | 2MB | 1000 | 1 | false | 369ms | 27068 | 16.64 | 59MB |
| mongodb→mariadb | small | 10K | 2MB | 1000 | 4 | false | 251ms | 39784 | 24.46 | 82MB |
| mongodb→cockroachdb | small | 10K | 2MB | 1000 | 1 | false | 2.308s | 788.2 | 2.70 | 60MB |
| mongodb→cockroachdb | small | 10K | 2MB | 1000 | 4 | false | 199ms | 9157.4 | 31.38 | 65MB |
| mongodb→mongodb | small | 10K | 2MB | 1000 | 1 | false | 310ms | 32230 | 22.84 | 56MB |
| mongodb→mongodb | small | 10K | 2MB | 1000 | 4 | false | 132ms | 75567 | 53.55 | 75MB |
| mongodb→redis | small | 10K | 2MB | 1000 | 1 | false | 148ms | 67456 | 53.96 | 48MB |
| mongodb→redis | small | 10K | 2MB | 1000 | 4 | false | 149ms | 67258 | 53.80 | 47MB |
| redis→sqlite | small | 10K | 2MB | 1000 | 1 | false | 910ms | 10983 | 6.98 | 60MB |
| redis→sqlite | small | 10K | 2MB | 1000 | 4 | false | 884ms | 11314 | 7.19 | 58MB |
| redis→postgres | small | 10K | 2MB | 1000 | 1 | false | 2.859s | 3497.3 | 2.28 | 69MB |
| redis→postgres | small | 10K | 2MB | 1000 | 4 | false | 929ms | 10769 | 7.03 | 65MB |
| redis→mysql | small | 10K | 2MB | 1000 | 1 | false | 918ms | 10896 | 6.92 | 68MB |
| redis→mysql | small | 10K | 2MB | 1000 | 4 | false | 922ms | 10849 | 6.90 | 65MB |
| redis→mariadb | small | 10K | 2MB | 1000 | 1 | false | 926ms | 10795 | 6.86 | 64MB |
| redis→mariadb | small | 10K | 2MB | 1000 | 4 | false | 891ms | 11220 | 7.13 | 63MB |
| redis→cockroachdb | small | 10K | 2MB | 1000 | 1 | false | 958ms | 10441 | 6.82 | 54MB |
| redis→cockroachdb | small | 10K | 2MB | 1000 | 4 | false | 1.809s | 5527.7 | 3.61 | 106MB |
| redis→mongodb | small | 10K | 2MB | 1000 | 1 | false | 918ms | 10889 | 8.76 | 54MB |
| redis→mongodb | small | 10K | 2MB | 1000 | 4 | false | 893ms | 11200 | 9.01 | 54MB |
| redis→redis | small | 10K | 2MB | 1000 | 1 | false | 902ms | 11084 | 6.37 | 49MB |
| redis→redis | small | 10K | 2MB | 1000 | 4 | false | 810ms | 12349 | 7.09 | 50MB |

### Resume Performance

| Size | Records | Before Interrupt | After Resume | Recovery Time | Correct |
|------|--------:|-----------------:|-------------:|--------------:|--------|
| small | 10K | 0 | 10K | 193ms | true |

### Parameter Analysis

#### small dataset (10K records)

**mariadb→mariadb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 30614 | - | 54MB |
| 4 | 0.33x | 41844 | 1.37x | 86MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mariadb→mongodb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 29423 | - | 56MB |
| 4 | 0.33x | 38690 | 1.31x | 58MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mariadb→mysql** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 18615 | - | 57MB |
| 4 | 0.33x | 25674 | 1.38x | 84MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mariadb→postgres** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 41015 | - | 53MB |
| 4 | 0.33x | 42070 | 1.03x | 53MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mariadb→redis** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 42047 | - | 47MB |
| 4 | 0.33x | 42112 | 1.00x | 48MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mariadb→sqlite** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 42143 | - | 56MB |
| 4 | 0.33x | 43183 | 1.02x | 59MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mongodb→cockroachdb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 788.2 | - | 60MB |
| 4 | 0.33x | 9157.4 | 11.62x | 65MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mongodb→mariadb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 27068 | - | 59MB |
| 4 | 0.33x | 39784 | 1.47x | 82MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mongodb→mongodb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 32230 | - | 56MB |
| 4 | 0.33x | 75567 | 2.34x | 75MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mongodb→mysql** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 17693 | - | 61MB |
| 4 | 0.33x | 30106 | 1.70x | 90MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mongodb→postgres** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 47931 | - | 58MB |
| 4 | 0.33x | 59863 | 1.25x | 57MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mongodb→redis** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 67456 | - | 48MB |
| 4 | 0.33x | 67258 | 1.00x | 47MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mongodb→sqlite** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 48976 | - | 58MB |
| 4 | 0.33x | 57186 | 1.17x | 72MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mysql→mariadb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 27441 | - | 59MB |
| 4 | 0.33x | 41085 | 1.50x | 74MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mysql→mongodb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 26536 | - | 56MB |
| 4 | 0.33x | 55565 | 2.09x | 75MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mysql→mysql** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 18742 | - | 59MB |
| 4 | 0.33x | 34423 | 1.84x | 87MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mysql→postgres** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 39913 | - | 53MB |
| 4 | 0.33x | 41654 | 1.04x | 52MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mysql→redis** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 95062 | - | 46MB |
| 4 | 0.33x | 101939 | 1.07x | 46MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**mysql→sqlite** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 39706 | - | 56MB |
| 4 | 0.33x | 44930 | 1.13x | 57MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**postgres→mariadb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 0.00 | - | 46MB |
| 4 | 0.33x | 0.00 | - | 47MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**postgres→mongodb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 0.00 | - | 45MB |
| 4 | 0.33x | 0.00 | - | 42MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**postgres→mysql** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 0.00 | - | 45MB |
| 4 | 0.33x | 0.00 | - | 42MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**postgres→postgres** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 0.00 | - | 39MB |
| 4 | 0.33x | 0.00 | - | 37MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**postgres→redis** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 0.00 | - | 39MB |
| 4 | 0.33x | 0.00 | - | 39MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**postgres→sqlite** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 0.00 | - | 44MB |
| 4 | 0.33x | 0.00 | - | 38MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**redis→cockroachdb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 10441 | - | 54MB |
| 4 | 0.33x | 5527.7 | 0.53x | 106MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**redis→mariadb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 10795 | - | 64MB |
| 4 | 0.33x | 11220 | 1.04x | 63MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**redis→mongodb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 10889 | - | 54MB |
| 4 | 0.33x | 11200 | 1.03x | 54MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**redis→mysql** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 10896 | - | 68MB |
| 4 | 0.33x | 10849 | 1.00x | 65MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**redis→postgres** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 3497.3 | - | 69MB |
| 4 | 0.33x | 10769 | 3.08x | 65MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**redis→redis** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 11084 | - | 49MB |
| 4 | 0.33x | 12349 | 1.11x | 50MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**redis→sqlite** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 10983 | - | 60MB |
| 4 | 0.33x | 11314 | 1.03x | 58MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**sqlite→mariadb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 29932 | - | 59MB |
| 4 | 0.33x | 41427 | 1.38x | 72MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**sqlite→mongodb** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 23941 | - | 57MB |
| 4 | 0.33x | 58328 | 2.44x | 79MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**sqlite→mysql** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 17501 | - | 57MB |
| 4 | 0.33x | 25580 | 1.46x | 82MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**sqlite→postgres** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 43246 | - | 52MB |
| 4 | 0.33x | 43530 | 1.01x | 50MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**sqlite→redis** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 95373 | - | 53MB |
| 4 | 0.33x | 100348 | 1.05x | 50MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

**sqlite→sqlite** — worker scaling comparison (batch=1000):

| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |
|--------:|----------:|-------:|--------:|---------:|
| 1 | 0.08x | 54649 | - | 52MB |
| 4 | 0.33x | 61312 | 1.12x | 86MB |

> With 12 CPU cores available: all tested worker counts fit within CPU capacity.

### Phase Breakdown

| Scenario | Size | Schema | Transfer | Verify |
|----------|------|-------:|---------:|-------:|
| sqlite→sqlite-small-b1000-w1-vfalse | small | 9ms | 0s | 0s |
| sqlite→sqlite-small-b1000-w4-vfalse | small | 11ms | 0s | 0s |
| sqlite→postgres-small-b1000-w1-vfalse | small | 1ms | 0s | 0s |
| sqlite→postgres-small-b1000-w4-vfalse | small | 0s | 0s | 0s |
| sqlite→mysql-small-b1000-w1-vfalse | small | 81ms | 0s | 0s |
| sqlite→mysql-small-b1000-w4-vfalse | small | 98ms | 0s | 0s |
| sqlite→mariadb-small-b1000-w1-vfalse | small | 19ms | 0s | 0s |
| sqlite→mariadb-small-b1000-w4-vfalse | small | 20ms | 0s | 0s |
| sqlite→mongodb-small-b1000-w1-vfalse | small | 43ms | 0s | 0s |
| sqlite→mongodb-small-b1000-w4-vfalse | small | 34ms | 0s | 0s |
| sqlite→redis-small-b1000-w1-vfalse | small | 0s | 0s | 0s |
| sqlite→redis-small-b1000-w4-vfalse | small | 0s | 0s | 0s |
| postgres→sqlite-small-b1000-w1-vfalse | small | 19ms | 0s | 0s |
| postgres→sqlite-small-b1000-w4-vfalse | small | 20ms | 0s | 0s |
| postgres→postgres-small-b1000-w1-vfalse | small | 4ms | 0s | 0s |
| postgres→postgres-small-b1000-w4-vfalse | small | 3ms | 0s | 0s |
| postgres→mysql-small-b1000-w1-vfalse | small | 102ms | 0s | 0s |
| postgres→mysql-small-b1000-w4-vfalse | small | 107ms | 0s | 0s |
| postgres→mariadb-small-b1000-w1-vfalse | small | 35ms | 0s | 0s |
| postgres→mariadb-small-b1000-w4-vfalse | small | 34ms | 0s | 0s |
| postgres→mongodb-small-b1000-w1-vfalse | small | 30ms | 0s | 0s |
| postgres→mongodb-small-b1000-w4-vfalse | small | 26ms | 0s | 0s |
| postgres→redis-small-b1000-w1-vfalse | small | 0s | 0s | 0s |
| postgres→redis-small-b1000-w4-vfalse | small | 0s | 0s | 0s |
| mysql→sqlite-small-b1000-w1-vfalse | small | 21ms | 0s | 0s |
| mysql→sqlite-small-b1000-w4-vfalse | small | 13ms | 0s | 0s |
| mysql→postgres-small-b1000-w1-vfalse | small | 3ms | 0s | 0s |
| mysql→postgres-small-b1000-w4-vfalse | small | 2ms | 0s | 0s |
| mysql→mysql-small-b1000-w1-vfalse | small | 63ms | 0s | 0s |
| mysql→mysql-small-b1000-w4-vfalse | small | 54ms | 0s | 0s |
| mysql→mariadb-small-b1000-w1-vfalse | small | 21ms | 0s | 0s |
| mysql→mariadb-small-b1000-w4-vfalse | small | 20ms | 0s | 0s |
| mysql→mongodb-small-b1000-w1-vfalse | small | 38ms | 0s | 0s |
| mysql→mongodb-small-b1000-w4-vfalse | small | 41ms | 0s | 0s |
| mysql→redis-small-b1000-w1-vfalse | small | 0s | 0s | 0s |
| mysql→redis-small-b1000-w4-vfalse | small | 0s | 0s | 0s |
| mariadb→sqlite-small-b1000-w1-vfalse | small | 13ms | 0s | 0s |
| mariadb→sqlite-small-b1000-w4-vfalse | small | 13ms | 0s | 0s |
| mariadb→postgres-small-b1000-w1-vfalse | small | 1ms | 0s | 0s |
| mariadb→postgres-small-b1000-w4-vfalse | small | 1ms | 0s | 0s |
| mariadb→mysql-small-b1000-w1-vfalse | small | 66ms | 0s | 0s |
| mariadb→mysql-small-b1000-w4-vfalse | small | 78ms | 0s | 0s |
| mariadb→mariadb-small-b1000-w1-vfalse | small | 22ms | 0s | 0s |
| mariadb→mariadb-small-b1000-w4-vfalse | small | 23ms | 0s | 0s |
| mariadb→mongodb-small-b1000-w1-vfalse | small | 1ms | 0s | 0s |
| mariadb→mongodb-small-b1000-w4-vfalse | small | 1ms | 0s | 0s |
| mariadb→redis-small-b1000-w1-vfalse | small | 0s | 0s | 0s |
| mariadb→redis-small-b1000-w4-vfalse | small | 0s | 0s | 0s |
| mongodb→sqlite-small-b1000-w1-vfalse | small | 0s | 0s | 0s |
| mongodb→sqlite-small-b1000-w4-vfalse | small | 0s | 0s | 0s |
| mongodb→postgres-small-b1000-w1-vfalse | small | 0s | 0s | 0s |
| mongodb→postgres-small-b1000-w4-vfalse | small | 0s | 0s | 0s |
| mongodb→mysql-small-b1000-w1-vfalse | small | 0s | 0s | 0s |
| mongodb→mysql-small-b1000-w4-vfalse | small | 0s | 0s | 0s |
| mongodb→mariadb-small-b1000-w1-vfalse | small | 0s | 0s | 0s |
| mongodb→mariadb-small-b1000-w4-vfalse | small | 0s | 0s | 0s |
| mongodb→cockroachdb-small-b1000-w1-vfalse | small | 0s | 0s | 0s |
| mongodb→cockroachdb-small-b1000-w4-vfalse | small | 0s | 0s | 0s |
| mongodb→mongodb-small-b1000-w1-vfalse | small | 0s | 0s | 0s |
| mongodb→mongodb-small-b1000-w4-vfalse | small | 0s | 0s | 0s |
| mongodb→redis-small-b1000-w1-vfalse | small | 0s | 0s | 0s |
| mongodb→redis-small-b1000-w4-vfalse | small | 0s | 0s | 0s |
| redis→sqlite-small-b1000-w1-vfalse | small | 0s | 0s | 0s |
| redis→sqlite-small-b1000-w4-vfalse | small | 0s | 0s | 0s |
| redis→postgres-small-b1000-w1-vfalse | small | 0s | 0s | 0s |
| redis→postgres-small-b1000-w4-vfalse | small | 0s | 0s | 0s |
| redis→mysql-small-b1000-w1-vfalse | small | 0s | 0s | 0s |
| redis→mysql-small-b1000-w4-vfalse | small | 0s | 0s | 0s |
| redis→mariadb-small-b1000-w1-vfalse | small | 0s | 0s | 0s |
| redis→mariadb-small-b1000-w4-vfalse | small | 0s | 0s | 0s |
| redis→cockroachdb-small-b1000-w1-vfalse | small | 0s | 0s | 0s |
| redis→cockroachdb-small-b1000-w4-vfalse | small | 0s | 0s | 0s |
| redis→mongodb-small-b1000-w1-vfalse | small | 0s | 0s | 0s |
| redis→mongodb-small-b1000-w4-vfalse | small | 0s | 0s | 0s |
| redis→redis-small-b1000-w1-vfalse | small | 0s | 0s | 0s |
| redis→redis-small-b1000-w4-vfalse | small | 0s | 0s | 0s |

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
