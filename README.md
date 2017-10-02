# dim_date_ir
Date dimension based on Iranian calendar
install required packages before using this script:

    $ pip install requirements

    $ python generate_dim.py -h

generate dim_date:


    $ python generate_dim.py --start '1999-01-01' --start '1999-02-01'

crawl events from time.ir:

    $ python generate_dim.py --crawl --start ...

only crawl data, dont generate dim_date file:

    $ python generate_dim.py --crawl-only --start ...
