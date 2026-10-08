import requests
import time
import json

error_commit = ["bd431c96e00814cf274b5f1f6c46c64f583e225e",
                "9f40f9d8e90cb222262ef477678f5012f86ae831",
                "63dc717298c8d42ef3e2dcf144b17c4bf06512d8",
                "24df863ec0f22cfa03dfe44b542041555fb60902",
                "c00b086a832ed881f51d24ffa0a0349c8b778120",
                "5ac093675beecee0e2285dfe51c6c19c038d76a8",
                "aa538f88b6444d3c4594608d24b56a2f93fc852d",
                "232e384606abe235abda5995dc086cf450312700",
                "8f3bd508e90d05d5a732c004254dc8cf9f97de40",
                "ecd64424f0a5d515830135528355c686155388f7",
                "bca932e110901fbaa9f29dcf981d05cd97b474b0",
                "f96a0936771b773b09e16ae0b2d92499b77e39f0",
                "1ebde4c2ca1bbc113776c285f15a57a2d618858c",
                "fb21c344c6d2b875a50576dcf1991df0b05f62cd",
                "ba3343bc4fa403a8dfbfcab7fc1a8c29ee34bd69",
                "d01a98b5afbc939f860c433a90a03ed17dfe74de",
                "66764b585adf075ab20b7494c04283707f6143ca",
                "27f27e92bdf2b140e8bb9b8a1c6e642c5d4a20f2",
                "422d1e47ccca37b0ccce61d3bbb957f24a73f5c5",
                "0f3d534237885f58cb336b112ea5af5aaa98f50f",
                "dfd645e78e158a80e5b363d1f324303609be48c7",
                "e892f06fb280012cf6ece80e993deb4c482c0a9c",
                "e403e55a6810ee67a31d8b066f16416ce365d28d",
                "b36a8bbcc8816993d55b69db4e0f74bcf1086243",
                "796ecc0c56941a87963f4d78e11975636a9dbfed",
                "f4c832f2b4fad84687c3506b232ab8d3b2dea081",
                "06f5cd728667bd7001f164da36f260a43f3b384c",
                "7a78b73c2133a8d8b851bc9542a5d3ba1b2b690f",
                "03e039cc75bdf4fe0ba50330ccb2fbad27135d5e",
                "114514a0c8259cd395b4e16acd78a89c2e317f5f",
                "4cfe4cb4e202f64de8745f9f5ea64723939fb7cd",
                "16319b9b5c3fecd40496d0d1728a5b7ffa53c3db",
                "faa32850f596d8db51028b48bdbc5463749a453f",
                "db4ba881c4e239b6717606272ae9131738111980",
                "ab3e4391b5f687d1a787f24263dc71e63e12203c",
                "4ff9acca1bde0d232b3401ea79901f8f4729a3e7",
                "e41517fb72f23d3c817028f0b47e2ef7de58f09c"
                ]

output_file = "./results/2024/error_data.res"

for error in error_commit:
    response = requests.get(f"https://archive.softwareheritage.org/api/1/revision/{error}/")
    data = response.json()
    print(data)
    with open(output_file, 'a') as f:
        json.dump(data, f)
        f.write('\n')
    time.sleep(1)