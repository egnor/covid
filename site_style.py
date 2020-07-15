# Module to set up website collateral (favicons, style sheets, etc).

import os
import pathlib
import zipfile


def write_icon_files(output_dir):
    """Writes favicon files into output_dir (and output_dir/icons)."""

    zip_path = pathlib.Path(__file__).parent / 'favicon_io.zip'
    out_path = pathlib.Path(output_dir)
    with zipfile.ZipFile(zip_path) as zip_file:
        os.makedirs(out_path / 'icons', exist_ok=True)
        for from_name, to_path in (
            ('favicon.ico', out_path / 'favicon.ico'),
            ('favicon-16x16.png', out_path / 'icons' / 'favicon-16x16.png'),
            ('favicon-32x32.png', out_path / 'icons' / 'favicon-32x32.png'),
        ):
            with zip_file.open(from_name) as read_file:
                with open(to_path, 'wb') as write_file:
                    write_file.write(read_file.read())


def add_icons_to_head(doc, path_to_root=''):
    """Adds links to favicon files (as written by write_icon_files())
    into a yattag doc, assuming the context is the document's <head>."""

    doc.stag('link', rel='icon', type='image/png', sizes='32x32',
             href=f'{path_to_root}icons/favicon-32x32.png')
    doc.stag('link', rel='icon', type='image/png', sizes='16x16',
             href=f'{path_to_root}icons/favicon-16x16.png')
