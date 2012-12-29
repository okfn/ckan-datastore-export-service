import os

import ckanserviceprovider.web as web

import csv_exporter

# check whether jobs have been imported properly
assert(csv_exporter.export_as_csv)


def serve():
    web.configure()
    web.run()


def serve_test():
    web.configure()
    return web.test_client()


def main():
    import argparse

    argparser = argparse.ArgumentParser(
        description='Service that allows exporting resources from the CKAN DataStore.',
        epilog='''"Would it save you a lot of time if I just gave up and went mad now?"''')

    argparser.add_argument('config', metavar='CONFIG', type=file,
                       help='configuration file')
    args = argparser.parse_args()

    os.environ['JOB_CONFIG'] = args.config.name
    serve()

if __name__ == '__main__':
    main()
