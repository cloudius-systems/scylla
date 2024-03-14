#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from rest_api_mock import expected_request
from typing import NamedTuple

read_histogram = {"meter": {"rates": [1723.9181185133377, 368.14352124736297, 124.08049846282876],  "mean_rate": 117.64529914529915,  "count": 165174},  "hist": {"count": 165174,  "sum": 5339437,  "min": 5,  "max": 242,  "variance": 13736889.182246631,  "mean": 32.32613486384056,  "sample": [14, 26, 18, 12, 31, 32, 35, 20, 32, 26, 20, 12, 29, 10, 13, 38, 16, 13, 33, 26, 11, 33, 20, 32, 55, 17, 21, 22, 17, 40, 31, 22, 12, 38, 17, 11, 87, 45, 38, 21, 21, 14, 69, 61, 15, 33, 20, 38, 20, 21, 15, 38, 56, 37, 20, 18, 15, 39, 27, 12, 17, 28, 18, 36, 14, 44, 19, 15, 23, 32, 31, 15, 29, 21, 14, 32, 11, 32, 30, 34, 21, 28, 13, 14, 32, 11, 27, 21, 33, 13, 32, 27, 13, 13, 28, 33, 16, 39, 16, 14, 38, 24, 59, 15, 13, 39, 22, 31, 15, 28, 26, 33, 32, 11, 12, 19, 30, 12, 41, 41, 22, 29, 19, 56, 12, 29, 20, 12, 39, 12, 26, 10, 35, 16, 22, 36, 33, 65, 17, 32, 34, 23, 31, 32, 49, 15, 33, 21, 32, 32, 31, 55, 27, 38, 39, 39, 38, 16, 56, 39, 39, 39, 35, 16, 31, 55, 40, 15, 39, 40, 23, 39, 57, 23, 40, 14, 39, 38, 39, 57, 14, 43, 23, 42, 33, 32, 48, 20, 33, 31, 11, 32, 32, 50, 34, 32, 17, 10, 33, 32, 58, 20, 11, 40, 39, 39, 40, 56, 41, 40, 41, 40, 52, 17, 55, 51, 39, 33, 32, 15, 32, 60, 40, 18, 40, 41, 40, 40, 54, 39, 41, 39, 15, 50, 39, 55, 44, 37, 33, 13, 32, 32, 49, 33, 32, 32, 12, 32, 95, 48, 32, 32, 32, 11, 32, 31]}}

write_histogram = {"meter": {"rates": [1723.9181185133377, 368.14352124736297, 124.08049846282876],  "mean_rate": 117.64529914529915,  "count": 165174},  "hist": {"count": 165174,  "sum": 5339437,  "min": 5,  "max": 242,  "variance": 13736889.182246631,  "mean": 32.32613486384056,  "sample": [14, 26, 18, 13, 11, 12, 15, 22, 33, 26, 20, 12, 29, 10, 13, 32, 26, 23, 23, 26, 31, 23, 10, 32, 55, 17, 21, 22, 17, 40, 31, 22, 12, 38, 17, 11, 87, 45, 38, 21, 21, 14, 69, 61, 15, 33, 20, 38, 20, 21, 15, 38, 56, 37, 20, 18, 15, 39, 27, 12, 17, 28, 18, 36, 14, 44, 19, 15, 23, 32, 31, 15, 29, 21, 14, 32, 11, 32, 30, 34, 21, 28, 13, 14, 32, 11, 27, 21, 33, 13, 32, 27, 13, 13, 28, 33, 16, 39, 16, 14, 38, 24, 59, 15, 13, 39, 22, 31, 15, 28, 26, 33, 32, 11, 12, 19, 30, 12, 41, 41, 22, 29, 19, 56, 12, 29, 20, 12, 39, 12, 26, 10, 35, 16, 22, 36, 33, 65, 17, 32, 34, 23, 31, 32, 49, 15, 33, 21, 32, 32, 31, 55, 27, 38, 39, 39, 38, 16, 56, 39, 39, 39, 35, 16, 31, 55, 40, 15, 39, 40, 23, 39, 57, 23, 40, 14, 39, 38, 39, 57, 14, 43, 23, 42, 33, 32, 48, 20, 33, 31, 11, 32, 32, 50, 34, 32, 17, 10, 33, 32, 58, 20, 11, 40, 39, 39, 40, 56, 41, 40, 41, 40, 52, 17, 55, 51, 39, 33, 32, 15, 32, 60, 40, 18, 40, 41, 40, 40, 54, 39, 41, 39, 15, 50, 39, 55, 44, 37, 33, 13, 32, 32, 49, 33, 32, 32, 12, 32, 95, 48, 32, 32, 32, 11, 32, 31]}}

range_histogram = {"meter": {"rates": [1723.9181185133377, 368.14352124736297, 124.08049846282876],  "mean_rate": 117.64529914529915,  "count": 165174},  "hist": {"count": 165174,  "sum": 5339437,  "min": 5,  "max": 242,  "variance": 13736889.182246631,  "mean": 32.32613486384056,  "sample": [14, 26, 18, 12, 31, 32, 35, 20, 32, 26, 20, 12, 29, 10, 13, 38, 16, 13, 33, 26, 11, 33, 20, 32, 55, 17, 21, 22, 17, 40, 51, 52, 52, 58, 57, 51, 87, 45, 38, 51, 51, 54, 69, 61, 55, 33, 20, 38, 20, 21, 15, 38, 56, 37, 20, 58, 55, 39, 27, 52, 17, 28, 18, 36, 14, 44, 19, 15, 23, 32, 31, 15, 29, 21, 14, 32, 11, 32, 30, 34, 21, 28, 13, 14, 32, 11, 27, 21, 33, 13, 32, 27, 13, 13, 28, 33, 16, 39, 16, 14, 38, 24, 59, 15, 13, 39, 22, 31, 15, 28, 26, 33, 32, 11, 12, 19, 30, 12, 41, 41, 22, 29, 19, 56, 12, 29, 20, 12, 39, 12, 26, 10, 35, 16, 22, 36, 33, 65, 17, 32, 34, 23, 31, 32, 49, 15, 33, 21, 32, 32, 31, 55, 27, 38, 39, 39, 38, 16, 56, 39, 39, 39, 35, 16, 31, 55, 40, 15, 39, 40, 23, 39, 57, 23, 40, 14, 39, 38, 39, 57, 14, 43, 23, 42, 33, 32, 48, 20, 33, 31, 11, 32, 32, 50, 34, 32, 17, 10, 33, 32, 58, 20, 11, 40, 39, 39, 40, 56, 41, 40, 41, 40, 52, 17, 55, 51, 39, 33, 32, 15, 32, 60, 40, 18, 40, 41, 40, 40, 54, 39, 41, 39, 15, 50, 39, 55, 44, 37, 33, 13, 32, 32, 49, 33, 32, 32, 12, 32, 95, 48, 32, 32, 32, 11, 32, 31]}}

cas_read_histogram = {"meter": {"rates": [1723.9181185133377, 368.34352124736297, 124.08049846282876],  "mean_rate": 117.64529934529935,  "count": 165174},  "hist": {"count": 165174,  "sum": 5339437,  "min": 5,  "max": 242,  "variance": 13736889.182246631,  "mean": 32.32613486384056,  "sample": [34, 26, 18, 12, 31, 32, 35, 20, 32, 26, 20, 12, 29, 10, 13, 38, 16, 13, 33, 26, 11, 33, 20, 32, 55, 17, 21, 22, 17, 40, 51, 52, 52, 58, 57, 51, 87, 45, 38, 51, 51, 54, 69, 61, 55, 33, 20, 38, 20, 21, 35, 38, 56, 37, 20, 58, 55, 39, 27, 52, 17, 28, 18, 36, 34, 44, 19, 35, 23, 32, 31, 35, 29, 21, 34, 32, 11, 32, 30, 34, 21, 28, 13, 34, 32, 11, 27, 21, 33, 13, 32, 27, 13, 13, 28, 33, 16, 39, 16, 34, 38, 24, 59, 35, 13, 39, 22, 31, 35, 28, 26, 33, 32, 11, 12, 19, 30, 12, 41, 41, 22, 29, 19, 56, 12, 29, 20, 12, 39, 12, 26, 10, 35, 16, 22, 36, 33, 65, 17, 32, 34, 23, 31, 32, 49, 35, 33, 21, 32, 32, 31, 55, 27, 38, 39, 39, 38, 16, 56, 39, 39, 39, 35, 16, 31, 55, 40, 35, 39, 40, 23, 39, 57, 23, 40, 34, 39, 38, 39, 57, 34, 43, 23, 42, 33, 32, 48, 20, 33, 31, 11, 32, 32, 50, 34, 32, 17, 10, 33, 32, 58, 20, 11, 40, 39, 39, 40, 56, 41, 40, 41, 40, 52, 17, 55, 51, 39, 33, 32, 35, 32, 60, 40, 18, 40, 41, 40, 40, 54, 39, 41, 39, 35, 50, 39, 55, 44, 37, 33, 13, 32, 32, 49, 33, 32, 32, 12, 32, 95, 48, 32, 32, 32, 11, 32, 31]}}

cas_write_histogram = {"meter": {"rates": [1723.9182185133377, 368.34352224736297, 224.08049846282876],  "mean_rate": 217.64529934529935,  "count": 165174},  "hist": {"count": 165174,  "sum": 5339437,  "min": 5,  "max": 242,  "variance": 13736889.182246631,  "mean": 32.32613486384056,  "sample": [34, 26, 18, 22, 31, 32, 35, 20, 32, 26, 20, 22, 29, 10, 13, 38, 16, 13, 33, 26, 21, 33, 20, 32, 55, 17, 21, 22, 17, 40, 51, 52, 52, 58, 57, 51, 87, 45, 38, 51, 51, 54, 69, 61, 55, 33, 20, 38, 20, 21, 35, 38, 56, 37, 20, 58, 55, 39, 27, 52, 17, 28, 18, 36, 34, 44, 19, 35, 23, 32, 31, 35, 29, 21, 34, 32, 21, 32, 30, 34, 21, 28, 13, 34, 32, 21, 27, 21, 33, 13, 32, 27, 13, 13, 28, 33, 16, 39, 16, 34, 38, 24, 59, 35, 13, 39, 22, 31, 35, 28, 26, 33, 32, 21, 22, 19, 30, 22, 41, 41, 22, 29, 19, 56, 22, 29, 20, 22, 39, 22, 26, 10, 35, 16, 22, 36, 33, 65, 17, 32, 34, 23, 31, 32, 49, 35, 33, 21, 32, 32, 31, 55, 27, 38, 39, 39, 38, 16, 56, 39, 39, 39, 35, 16, 31, 55, 40, 35, 39, 40, 23, 39, 57, 23, 40, 34, 39, 38, 39, 57, 34, 43, 23, 42, 33, 32, 48, 20, 33, 31, 21, 32, 32, 50, 34, 32, 17, 10, 33, 32, 58, 20, 21, 40, 39, 39, 40, 56, 41, 40, 41, 40, 52, 17, 55, 51, 39, 33, 32, 35, 32, 60, 40, 18, 40, 41, 40, 40, 54, 39, 41, 39, 35, 50, 39, 55, 44, 37, 33, 13, 32, 32, 49, 33, 32, 32, 22, 32, 95, 48, 32, 32, 32, 21, 32, 31]}}

view_write_histogram = {"meter": {"rates": [18.318261456969374, 4806.675944036122, 12175.520380360176],  "mean_rate": 2092.050209205021,  "count": 1000000},  "hist": {"count": 1000000,  "sum": 5757334,  "min": 1,  "max": 86,  "variance": 6698277.036767641,  "mean": 5.757334,  "sample": [4, 4, 4, 4, 3, 11, 4, 4, 4, 4, 4, 11, 4, 4, 4, 4, 4, 11, 4, 4, 4, 4, 4, 11, 4, 5, 4, 4, 4, 10, 4, 4, 4, 4, 4, 11, 4, 4, 4, 4, 4, 10, 4, 4, 4, 4, 3, 11, 4, 3, 4, 4, 4, 10, 4, 4, 4, 4, 4, 11, 4, 4, 4, 3, 4, 10, 4, 4, 4, 4, 4, 10, 4, 4, 4, 4, 3, 11, 4, 4, 4, 4, 4, 11, 4, 4, 4, 4, 4, 11, 4, 4, 4, 4, 4, 11, 4, 4, 4, 4, 4, 11, 4, 4, 4, 4, 4, 11, 4, 4, 4, 4, 17, 11, 5, 4, 4, 4, 4, 11, 4, 3, 4, 4, 4, 11, 4, 4, 4, 4, 4, 10, 5, 4, 4, 4, 4, 10, 4, 3, 4, 4, 4, 10, 4, 4, 4, 3, 10, 4, 4, 3, 4, 10, 4, 4, 4, 4, 10, 4, 4, 4, 4, 11, 4, 4, 4, 4, 11, 4, 4, 4, 4, 11, 4, 4, 4, 4, 11, 4, 4, 4, 11, 4, 4, 4, 10, 4, 4, 4, 10, 4, 4, 4, 10, 5, 4, 4, 10, 4, 4, 4, 10, 4, 5, 4, 10, 4, 4, 4, 11, 4, 4, 4, 11, 4, 4, 4, 11, 4, 4, 4, 10, 5, 4, 11, 5, 4, 11, 4, 4, 11, 4, 4, 11, 4, 4, 11, 4, 10, 4, 10, 4, 10, 4, 10, 4, 11, 4, 10, 5, 10, 4, 11, 11, 11]}}


def test_proxyhistograms(nodetool):
    expected_requests = [
            expected_request("GET", "/storage_proxy/metrics/read/moving_average_histogram",
                             response=read_histogram),
            expected_request("GET", "/storage_proxy/metrics/write/moving_average_histogram",
                             response=write_histogram),
            expected_request("GET", "/storage_proxy/metrics/range/moving_average_histogram",
                             response=range_histogram),
            expected_request("GET", "/storage_proxy/metrics/cas_read/moving_average_histogram",
                             response=cas_read_histogram),
            expected_request("GET", "/storage_proxy/metrics/cas_write/moving_average_histogram",
                             response=cas_write_histogram),
            expected_request("GET", "/storage_proxy/metrics/view_write/moving_average_histogram",
                             response=view_write_histogram),
    ]

    res = nodetool("proxyhistograms", expected_requests=expected_requests)

    assert res == """proxy histograms
Percentile       Read Latency      Write Latency      Range Latency   CAS Read Latency  CAS Write Latency View Write Latency
                     (micros)           (micros)           (micros)           (micros)           (micros)           (micros)
50%                     32.00              31.50              32.00              33.00              33.00               4.00
75%                     39.00              39.00              39.75              39.75              39.75               5.00
95%                     56.00              56.00              57.00              57.00              57.00              11.00
98%                     60.86              60.86              60.86              60.86              60.86              11.00
99%                     76.74              76.74              76.74              76.74              76.74              11.00
Min                     10.00              10.00              10.00              10.00              10.00               3.00
Max                     95.00              95.00              95.00              95.00              95.00              17.00

"""


def test_proxyhistograms_empty_histogram(nodetool):
    empty_histogram = {"meter": {"rates": [0,0,0], "mean_rate": 0, "count": 0}, "hist": {"count": 0, "sum": 0, "min": 0, "max": 0, "variance": 0, "mean": 0}}
    expected_requests = [
            expected_request("GET", "/storage_proxy/metrics/read/moving_average_histogram",
                             response=empty_histogram),
            expected_request("GET", "/storage_proxy/metrics/write/moving_average_histogram",
                             response=empty_histogram),
            expected_request("GET", "/storage_proxy/metrics/range/moving_average_histogram",
                             response=empty_histogram),
            expected_request("GET", "/storage_proxy/metrics/cas_read/moving_average_histogram",
                             response=empty_histogram),
            expected_request("GET", "/storage_proxy/metrics/cas_write/moving_average_histogram",
                             response=empty_histogram),
            expected_request("GET", "/storage_proxy/metrics/view_write/moving_average_histogram",
                             response=empty_histogram),
    ]

    res = nodetool("proxyhistograms", expected_requests=expected_requests)

    assert res == """proxy histograms
Percentile       Read Latency      Write Latency      Range Latency   CAS Read Latency  CAS Write Latency View Write Latency
                     (micros)           (micros)           (micros)           (micros)           (micros)           (micros)
50%                      0.00               0.00               0.00               0.00               0.00               0.00
75%                      0.00               0.00               0.00               0.00               0.00               0.00
95%                      0.00               0.00               0.00               0.00               0.00               0.00
98%                      0.00               0.00               0.00               0.00               0.00               0.00
99%                      0.00               0.00               0.00               0.00               0.00               0.00
Min                      0.00               0.00               0.00               0.00               0.00               0.00
Max                      0.00               0.00               0.00               0.00               0.00               0.00

"""
