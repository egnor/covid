"""Module to set up website collateral (favicons, style sheets, etc)."""

import os
import pathlib
import zipfile

from dominate import tags, util

from covid import urls


def write_style_files(site_dir):
    """Writes CSS & favicon files into site_dir."""

    # Extract favicon files from the favicon_io.zip in the source directory.
    source_dir = pathlib.Path(__file__).parent
    with zipfile.ZipFile(source_dir / "favicon_io.zip") as zip_file:
        for name in ("favicon.ico", "favicon-16x16.png", "favicon-32x32.png"):
            with zip_file.open(name) as read_file:
                with open(urls.file(site_dir, name), "wb") as write_file:
                    write_file.write(read_file.read())

    # Copy style files directly from the source directory.
    for name in ("style.css", "NotoColorEmoji.ttf", "video.js"):
        with open(source_dir / name, "rb") as read_file:
            with open(urls.file(site_dir, name), "wb") as write_file:
                write_file.write(read_file.read())


def add_head_style(this_urlpath=""):
    """Adds <link> tags for style files, assuming <head> context."""

    tags.meta(charset="utf-8")
    tags.meta(name="viewport", content="width=device-width, initial-scale=1.0")

    emoji = urls.link(this_urlpath, "NotoColorEmoji.ttf")
    tags.style(
        f"""
        @font-face {{
            font-family: 'Noto Color Emoji';
            src: local('Noto Color Emoji'), url({emoji}) format("truetype");'
        }}
    """
    )

    tags.link(
        rel="icon",
        type="image/png",
        sizes="32x32",
        href=urls.link(this_urlpath, "favicon-32x32.png"),
    )
    tags.link(
        rel="icon",
        type="image/png",
        sizes="16x16",
        href=urls.link(this_urlpath, "favicon-16x16.png"),
    )
    tags.link(
        rel="stylesheet",
        type="text/css",
        href=urls.link(this_urlpath, "style.css"),
    )
    tags.script(src=urls.link(this_urlpath, "video.js"))
    tags.script(
        src="https://kit.fontawesome.com/7e1cde4d00.js", crossorigin="anonymous"
    )
