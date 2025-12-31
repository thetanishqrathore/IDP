# type: ignore
"""
Generic Document Parser Utility (Ported from RAG-Anything)

This module provides functionality for parsing PDF and image documents using MinerU 2.0 library
and Docling, converting the parsing results into structured formats.
"""

from __future__ import annotations

import json
import argparse
import base64
import subprocess
import tempfile
import logging
from pathlib import Path
from typing import (
    Dict,
    List,
    Optional,
    Union,
    Tuple,
    Any,
    TypeVar,
)

T = TypeVar("T")


class MineruExecutionError(Exception):
    """catch mineru error"""

    def __init__(self, return_code, error_msg):
        self.return_code = return_code
        self.error_msg = error_msg
        super().__init__(
            f"Mineru command failed with return code {return_code}: {error_msg}"
        )


class Parser:
    """
    Base class for document parsing utilities.
    """

    # Define common file formats
    OFFICE_FORMATS = {'.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx'}
    IMAGE_FORMATS = {'.png', '.jpeg', '.jpg', '.bmp', '.tiff', '.tif', '.gif', '.webp'}
    TEXT_FORMATS = {'.txt', '.md'}

    # Class-level logger
    logger = logging.getLogger(__name__)

    def __init__(self) -> None:
        """Initialize the base parser."""
        pass

    @staticmethod
    def convert_office_to_pdf(
        doc_path: Union[str, Path],
        output_dir: Optional[str] = None,
    ) -> Path:
        """
        Convert Office document (.doc, .docx, .ppt, .pptx, .xls, .xlsx) to PDF.
        Requires LibreOffice to be installed.
        """
        try:
            # Convert to Path object for easier handling
            doc_path = Path(doc_path)
            if not doc_path.exists():
                raise FileNotFoundError(f"Office document does not exist: {doc_path}")

            name_without_suff = doc_path.stem

            # Prepare output directory
            if output_dir:
                base_output_dir = Path(output_dir)
            else:
                base_output_dir = doc_path.parent / "libreoffice_output"

            base_output_dir.mkdir(parents=True, exist_ok=True)

            # Create temporary directory for PDF conversion
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)

                # Prepare subprocess parameters to hide console window on Windows
                import platform

                # Try LibreOffice commands in order of preference
                commands_to_try = ["libreoffice", "soffice"]

                conversion_successful = False
                for cmd in commands_to_try:
                    try:
                        convert_cmd = [
                            cmd,
                            "--headless",
                            "--convert-to",
                            "pdf",
                            "--outdir",
                            str(temp_path),
                            str(doc_path),
                        ]

                        # Prepare conversion subprocess parameters
                        convert_subprocess_kwargs = {
                            "capture_output": True,
                            "text": True,
                            "timeout": 60,  # 60 second timeout
                            "encoding": "utf-8",
                            "errors": "ignore",
                        }

                        # Hide console window on Windows
                        if platform.system() == "Windows":
                            convert_subprocess_kwargs["creationflags"] = (
                                subprocess.CREATE_NO_WINDOW
                            )

                        result = subprocess.run(
                            convert_cmd, **convert_subprocess_kwargs
                        )

                        if result.returncode == 0:
                            conversion_successful = True
                            break
                    except Exception:
                        continue

                if not conversion_successful:
                    raise RuntimeError(
                        f"LibreOffice conversion failed for {doc_path.name}. "
                        "Please ensure LibreOffice is installed."
                    )

                # Find the generated PDF
                pdf_files = list(temp_path.glob("*.pdf"))
                if not pdf_files:
                    raise RuntimeError(
                        f"PDF conversion failed for {doc_path.name} - no PDF file generated."
                    )

                pdf_path = pdf_files[0]

                # Validate the generated PDF
                if pdf_path.stat().st_size < 100:  # Very small file, likely empty
                    raise RuntimeError(
                        "Generated PDF appears to be empty or corrupted."
                    )

                # Copy PDF to final output directory
                final_pdf_path = base_output_dir / f"{name_without_suff}.pdf"
                import shutil

                shutil.copy2(pdf_path, final_pdf_path)

                return final_pdf_path

        except Exception as e:
            logging.error(f"Error in convert_office_to_pdf: {str(e)}")
            raise

    @staticmethod
    def convert_text_to_pdf(
        text_path: Union[str, Path],
        output_dir: Optional[str] = None,
    ) -> Path:
        """
        Convert text file (.txt, .md) to PDF using ReportLab with full markdown support.
        """
        try:
            text_path = Path(text_path)
            if not text_path.exists():
                raise FileNotFoundError(f"Text file does not exist: {text_path}")

            # Supported text formats
            supported_text_formats = {'.txt', '.md'}
            if text_path.suffix.lower() not in supported_text_formats:
                raise ValueError(f"Unsupported text format: {text_path.suffix}")

            # Read the text content
            try:
                with open(text_path, "r", encoding="utf-8") as f:
                    text_content = f.read()
            except UnicodeDecodeError:
                # Try with different encodings
                for encoding in ["gbk", "latin-1", "cp1252"]:
                    try:
                        with open(text_path, "r", encoding=encoding) as f:
                            text_content = f.read()
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    raise RuntimeError(
                        f"Could not decode text file {text_path.name} with any supported encoding"
                    )

            # Prepare output directory
            if output_dir:
                base_output_dir = Path(output_dir)
            else:
                base_output_dir = text_path.parent / "reportlab_output"

            base_output_dir.mkdir(parents=True, exist_ok=True)
            pdf_path = base_output_dir / f"{text_path.stem}.pdf"

            try:
                from reportlab.lib.pagesizes import A4
                from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
                from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
                from reportlab.lib.units import inch
                from reportlab.pdfbase import pdfmetrics
                from reportlab.pdfbase.ttfonts import TTFont

                support_chinese = True
                try:
                    # Attempt to register fonts if available (skipped for brevity/robustness in port)
                    pass
                except Exception:
                    support_chinese = False

                # Create PDF document
                doc = SimpleDocTemplate(
                    str(pdf_path),
                    pagesize=A4,
                    leftMargin=inch,
                    rightMargin=inch,
                    topMargin=inch,
                    bottomMargin=inch,
                )

                # Get styles
                styles = getSampleStyleSheet()
                normal_style = styles["Normal"]
                heading_style = styles["Heading1"]

                # Build content
                story = []

                # Handle markdown or plain text
                if text_path.suffix.lower() == ".md":
                    lines = text_content.split("\n")
                    for line in lines:
                        line = line.strip()
                        if not line:
                            story.append(Spacer(1, 12))
                            continue

                        # Headers
                        if line.startswith("#"):
                            level = len(line) - len(line.lstrip("#"))
                            header_text = line.lstrip("#").strip()
                            if header_text:
                                header_style = ParagraphStyle(
                                    name=f"Heading{level}",
                                    parent=heading_style,
                                    fontSize=max(16 - level, 10),
                                    spaceAfter=8,
                                    spaceBefore=16 if level <= 2 else 12,
                                )
                                story.append(Paragraph(header_text, header_style))
                        else:
                            # Regular text
                            story.append(Paragraph(line, normal_style))
                            story.append(Spacer(1, 6))
                else:
                    # Handle plain text files (.txt)
                    lines = text_content.split("\n")
                    for line in lines:
                        line = line.rstrip()
                        if not line.strip():
                            story.append(Spacer(1, 6))
                            continue
                        safe_line = (
                            line.replace("&", "&amp;")
                            .replace("<", "&lt;")
                            .replace(">", "&gt;")
                        )
                        story.append(Paragraph(safe_line, normal_style))
                        story.append(Spacer(1, 3))

                if not story:
                    story.append(Paragraph("(Empty text file)", normal_style))

                doc.build(story)

            except ImportError:
                raise RuntimeError(
                    "reportlab is required for text-to-PDF conversion. "
                    "Please install it using: pip install reportlab"
                )
            except Exception as e:
                raise RuntimeError(
                    f"Failed to convert text file {text_path.name} to PDF: {str(e)}"
                )

            # Validate the generated PDF
            if not pdf_path.exists() or pdf_path.stat().st_size < 100:
                raise RuntimeError(
                    f"PDF conversion failed for {text_path.name} - generated PDF is empty or corrupted."
                )

            return pdf_path

        except Exception as e:
            logging.error(f"Error in convert_text_to_pdf: {str(e)}")
            raise

    def parse_pdf(self, pdf_path: Union[str, Path], output_dir: Optional[str] = None, method: str = "auto", lang: Optional[str] = None, **kwargs) -> List[Dict[str, Any]]:
        raise NotImplementedError("parse_pdf must be implemented by subclasses")

    def parse_image(self, image_path: Union[str, Path], output_dir: Optional[str] = None, lang: Optional[str] = None, **kwargs) -> List[Dict[str, Any]]:
        raise NotImplementedError("parse_image must be implemented by subclasses")

    def parse_document(self, file_path: Union[str, Path], method: str = "auto", output_dir: Optional[str] = None, lang: Optional[str] = None, **kwargs) -> List[Dict[str, Any]]:
        raise NotImplementedError("parse_document must be implemented by subclasses")

    def check_installation(self) -> bool:
        raise NotImplementedError("check_installation must be implemented by subclasses")


class MineruParser(Parser):
    """
    MinerU 2.0 document parsing utility class
    """

    __slots__ = ()
    logger = logging.getLogger(__name__)

    def __init__(self) -> None:
        super().__init__()

    @staticmethod
    def _run_mineru_command(
        input_path: Union[str, Path],
        output_dir: Union[str, Path],
        method: str = "auto",
        lang: Optional[str] = None,
        backend: Optional[str] = None,
        start_page: Optional[int] = None,
        end_page: Optional[int] = None,
        formula: bool = True,
        table: bool = True,
        device: Optional[str] = None,
        source: Optional[str] = None,
        vlm_url: Optional[str] = None,
    ) -> None:
        cmd = [
            "mineru",
            "-p", str(input_path),
            "-o", str(output_dir),
            "-m", method,
        ]

        if backend: cmd.extend(["-b", backend])
        if source: cmd.extend(["--source", source])
        if lang: cmd.extend(["-l", lang])
        if start_page is not None: cmd.extend(["-s", str(start_page)])
        if end_page is not None: cmd.extend(["-e", str(end_page)])
        if not formula: cmd.extend(["-f", "false"])
        if not table: cmd.extend(["-t", "false"])
        if device: cmd.extend(["-d", device])
        if vlm_url: cmd.extend(["-u", vlm_url])

        output_lines = []
        error_lines = []

        try:
            import platform
            import threading
            from queue import Queue, Empty

            subprocess_kwargs = {
                "stdout": subprocess.PIPE,
                "stderr": subprocess.PIPE,
                "text": True,
                "encoding": "utf-8",
                "errors": "ignore",
                "bufsize": 1,
            }
            if platform.system() == "Windows":
                subprocess_kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW

            def enqueue_output(pipe, queue, prefix):
                try:
                    for line in iter(pipe.readline, ""):
                        if line.strip():
                            queue.put((prefix, line.strip()))
                    pipe.close()
                except Exception:
                    pass

            process = subprocess.Popen(cmd, **subprocess_kwargs)
            stdout_queue = Queue()
            stderr_queue = Queue()

            stdout_thread = threading.Thread(target=enqueue_output, args=(process.stdout, stdout_queue, "STDOUT"))
            stderr_thread = threading.Thread(target=enqueue_output, args=(process.stderr, stderr_queue, "STDERR"))
            stdout_thread.daemon = True
            stderr_thread.daemon = True
            stdout_thread.start()
            stderr_thread.start()

            while process.poll() is None:
                try:
                    while True:
                        _, line = stdout_queue.get_nowait()
                        output_lines.append(line)
                except Empty:
                    pass
                try:
                    while True:
                        _, line = stderr_queue.get_nowait()
                        if "error" in line.lower():
                            error_lines.append(line)
                except Empty:
                    pass
                import time
                time.sleep(0.1)

            # drain
            try:
                while True:
                    _, line = stdout_queue.get_nowait()
                    output_lines.append(line)
            except Empty: pass
            try:
                while True:
                    _, line = stderr_queue.get_nowait()
                    if "error" in line.lower():
                        error_lines.append(line)
            except Empty: pass

            return_code = process.wait()
            stdout_thread.join(timeout=2)
            stderr_thread.join(timeout=2)

            if return_code != 0 or error_lines:
                raise MineruExecutionError(return_code, error_lines)

        except MineruExecutionError:
            raise
        except FileNotFoundError:
            raise RuntimeError("mineru command not found. Please ensure 'mineru[core]' is installed.")
        except Exception as e:
            raise RuntimeError(f"Unexpected error running mineru command: {e}") from e

    @staticmethod
    def _read_output_files(output_dir: Path, file_stem: str, method: str = "auto") -> Tuple[List[Dict[str, Any]], str]:
        md_file = output_dir / f"{file_stem}.md"
        json_file = output_dir / f"{file_stem}_content_list.json"
        images_base_dir = output_dir

        file_stem_subdir = output_dir / file_stem
        if file_stem_subdir.exists():
            md_file = file_stem_subdir / method / f"{file_stem}.md"
            json_file = file_stem_subdir / method / f"{file_stem}_content_list.json"
            images_base_dir = file_stem_subdir / method

        md_content = ""
        if md_file.exists():
            try:
                with open(md_file, "r", encoding="utf-8") as f:
                    md_content = f.read()
            except Exception:
                pass

        content_list = []
        if json_file.exists():
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    content_list = json.load(f)
                
                # Fix relative paths
                for item in content_list:
                    if isinstance(item, dict):
                        for field_name in ["img_path", "table_img_path", "equation_img_path"]:
                            if field_name in item and item[field_name]:
                                item[field_name] = str((images_base_dir / item[field_name]).resolve())
            except Exception:
                pass

        return content_list, md_content

    def parse_pdf(self, pdf_path: Union[str, Path], output_dir: Optional[str] = None, method: str = "auto", lang: Optional[str] = None, **kwargs) -> List[Dict[str, Any]]:
        try:
            pdf_path = Path(pdf_path)
            if not pdf_path.exists():
                raise FileNotFoundError(f"PDF file does not exist: {pdf_path}")
            name_without_suff = pdf_path.stem

            if output_dir:
                base_output_dir = Path(output_dir)
            else:
                base_output_dir = pdf_path.parent / "mineru_output"
            base_output_dir.mkdir(parents=True, exist_ok=True)

            self._run_mineru_command(
                input_path=pdf_path,
                output_dir=base_output_dir,
                method=method,
                lang=lang,
                **kwargs,
            )

            backend = kwargs.get("backend", "")
            if backend.startswith("vlm-"):
                method = "vlm"

            content_list, md_content = self._read_output_files(base_output_dir, name_without_suff, method=method)
            return {"content_list": content_list, "markdown": md_content}
        except Exception as e:
            logging.error(f"Error in parse_pdf: {str(e)}")
            raise

    def parse_image(self, image_path: Union[str, Path], output_dir: Optional[str] = None, lang: Optional[str] = None, **kwargs) -> List[Dict[str, Any]]:
        try:
            image_path = Path(image_path)
            if not image_path.exists():
                raise FileNotFoundError(f"Image file does not exist: {image_path}")

            # Conversion logic for non-standard formats
            mineru_supported = {'.png', '.jpeg', '.jpg'}
            ext = image_path.suffix.lower()
            actual_image_path = image_path
            temp_converted_file = None

            if ext not in mineru_supported:
                try:
                    from PIL import Image
                    temp_dir = Path(tempfile.mkdtemp())
                    temp_converted_file = temp_dir / f"{image_path.stem}_converted.png"
                    with Image.open(image_path) as img:
                        img.convert("RGB").save(temp_converted_file, "PNG")
                    actual_image_path = temp_converted_file
                except Exception as e:
                    if temp_converted_file and temp_converted_file.exists():
                        temp_converted_file.unlink()
                    raise RuntimeError(f"Failed to convert image {image_path.name}: {str(e)}")

            name_without_suff = image_path.stem
            if output_dir:
                base_output_dir = Path(output_dir)
            else:
                base_output_dir = image_path.parent / "mineru_output"
            base_output_dir.mkdir(parents=True, exist_ok=True)

            try:
                self._run_mineru_command(
                    input_path=actual_image_path,
                    output_dir=base_output_dir,
                    method="ocr",
                    lang=lang,
                    **kwargs,
                )
                content_list, md_content = self._read_output_files(base_output_dir, name_without_suff, method="ocr")
                return {"content_list": content_list, "markdown": md_content}
            finally:
                if temp_converted_file and temp_converted_file.exists():
                    try:
                        temp_converted_file.unlink()
                        temp_converted_file.parent.rmdir()
                    except Exception:
                        pass
        except Exception as e:
            logging.error(f"Error in parse_image: {str(e)}")
            raise

    def parse_office_doc(self, doc_path: Union[str, Path], output_dir: Optional[str] = None, lang: Optional[str] = None, **kwargs) -> List[Dict[str, Any]]:
        pdf_path = self.convert_office_to_pdf(doc_path, output_dir)
        return self.parse_pdf(pdf_path=pdf_path, output_dir=output_dir, lang=lang, **kwargs)

    def parse_text_file(self, text_path: Union[str, Path], output_dir: Optional[str] = None, lang: Optional[str] = None, **kwargs) -> List[Dict[str, Any]]:
        pdf_path = self.convert_text_to_pdf(text_path, output_dir)
        return self.parse_pdf(pdf_path=pdf_path, output_dir=output_dir, lang=lang, **kwargs)

    def parse_document(self, file_path: Union[str, Path], method: str = "auto", output_dir: Optional[str] = None, lang: Optional[str] = None, **kwargs) -> List[Dict[str, Any]]:
        file_path = Path(file_path)
        ext = file_path.suffix.lower()

        if ext == ".pdf":
            return self.parse_pdf(file_path, output_dir, method, lang, **kwargs)
        elif ext in self.IMAGE_FORMATS:
            return self.parse_image(file_path, output_dir, lang, **kwargs)
        elif ext in self.OFFICE_FORMATS:
            return self.parse_office_doc(file_path, output_dir, lang, **kwargs)
        elif ext in self.TEXT_FORMATS:
            return self.parse_text_file(file_path, output_dir, lang, **kwargs)
        else:
            return self.parse_pdf(file_path, output_dir, method, lang, **kwargs)

    def check_installation(self) -> bool:
        try:
            subprocess.run(["mineru", "--version"], capture_output=True, check=True)
            return True
        except Exception:
            return False


class DoclingParser(Parser):
    """
    Docling document parsing utility class.
    """
    HTML_FORMATS = {'.html', '.htm', '.xhtml'}

    def __init__(self) -> None:
        super().__init__()

    def parse_pdf(self, pdf_path: Union[str, Path], output_dir: Optional[str] = None, method: str = "auto", lang: Optional[str] = None, **kwargs) -> List[Dict[str, Any]]:
        try:
            pdf_path = Path(pdf_path)
            if not pdf_path.exists():
                raise FileNotFoundError(f"PDF file does not exist: {pdf_path}")
            name_without_suff = pdf_path.stem

            if output_dir:
                base_output_dir = Path(output_dir)
            else:
                base_output_dir = pdf_path.parent / "docling_output"
            base_output_dir.mkdir(parents=True, exist_ok=True)

            self._run_docling_command(input_path=pdf_path, output_dir=base_output_dir, file_stem=name_without_suff, **kwargs)
            content_list, md_content = self._read_output_files(base_output_dir, name_without_suff)
            return {"content_list": content_list, "markdown": md_content}
        except Exception as e:
            logging.error(f"Error in parse_pdf: {str(e)}")
            raise

    def parse_document(self, file_path: Union[str, Path], method: str = "auto", output_dir: Optional[str] = None, lang: Optional[str] = None, **kwargs) -> List[Dict[str, Any]]:
        file_path = Path(file_path)
        ext = file_path.suffix.lower()

        if ext == ".pdf":
            return self.parse_pdf(file_path, output_dir, method, lang, **kwargs)
        elif ext in self.OFFICE_FORMATS:
            return self.parse_office_doc(file_path, output_dir, lang, **kwargs)
        elif ext in self.HTML_FORMATS:
            return self.parse_html(file_path, output_dir, lang, **kwargs)
        else:
            raise ValueError(f"Unsupported format for Docling: {ext}")

    def _run_docling_command(self, input_path: Union[str, Path], output_dir: Union[str, Path], file_stem: str, **kwargs) -> None:
        file_output_dir = Path(output_dir) / file_stem / "docling"
        file_output_dir.mkdir(parents=True, exist_ok=True)

        cmd_json = ["docling", "--output", str(file_output_dir), "--to", "json", str(input_path)]
        cmd_md = ["docling", "--output", str(file_output_dir), "--to", "md", str(input_path)]

        import platform
        kwargs = {"capture_output": True, "text": True, "check": True, "encoding": "utf-8", "errors": "ignore"}
        if platform.system() == "Windows":
            kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
        
        try:
            subprocess.run(cmd_json, **kwargs)
            subprocess.run(cmd_md, **kwargs)
        except subprocess.CalledProcessError as e:
             logging.error(f"Docling failed: {e.stderr}")
             raise
        except FileNotFoundError:
             raise RuntimeError("docling command not found")

    def _read_output_files(self, output_dir: Path, file_stem: str) -> Tuple[List[Dict[str, Any]], str]:
        file_subdir = output_dir / file_stem / "docling"
        md_file = file_subdir / f"{file_stem}.md"
        json_file = file_subdir / f"{file_stem}.json"

        md_content = ""
        if md_file.exists():
            try:
                with open(md_file, "r", encoding="utf-8") as f:
                    md_content = f.read()
            except Exception: pass

        content_list = []
        if json_file.exists():
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    docling_content = json.load(f)
                    content_list = self.read_from_block_recursive(docling_content.get("body", {}), "body", file_subdir, 0, "0", docling_content)
            except Exception: pass
        return content_list, md_content

    def read_from_block_recursive(self, block, type: str, output_dir: Path, cnt: int, num: str, docling_content: Dict[str, Any]) -> List[Dict[str, Any]]:
        content_list = []
        if not block.get("children"):
            cnt += 1
            content_list.append(self.read_from_block(block, type, output_dir, cnt, num))
        else:
            if type not in ["groups", "body"]:
                cnt += 1
                content_list.append(self.read_from_block(block, type, output_dir, cnt, num))
            members = block["children"]
            for member in members:
                cnt += 1
                try:
                    member_tag = member["$ref"]
                    parts = member_tag.split("/")
                    member_type = parts[1]
                    member_num = parts[2]
                    member_block = docling_content[member_type][int(member_num)]
                    content_list.extend(self.read_from_block_recursive(member_block, member_type, output_dir, cnt, member_num, docling_content))
                except Exception:
                    pass
        return content_list

    def read_from_block(self, block, type: str, output_dir: Path, cnt: int, num: str) -> Dict[str, Any]:
        if type == "texts":
            if block.get("label") == "formula":
                return {"type": "equation", "img_path": "", "text": block.get("orig", ""), "page_idx": cnt // 10}
            else:
                return {"type": "text", "text": block.get("orig", ""), "page_idx": cnt // 10}
        elif type == "pictures":
            try:
                base64_uri = block["image"]["uri"]
                base64_str = base64_uri.split(",")[1]
                image_dir = output_dir / "images"
                image_dir.mkdir(parents=True, exist_ok=True)
                image_path = image_dir / f"image_{num}.png"
                with open(image_path, "wb") as f:
                    f.write(base64.b64decode(base64_str))
                return {
                    "type": "image",
                    "img_path": str(image_path.resolve()),
                    "image_caption": block.get("caption", ""),
                    "page_idx": cnt // 10,
                }
            except Exception:
                return {"type": "text", "text": "[Image failed]", "page_idx": cnt // 10}
        else:
             return {
                "type": "table",
                "img_path": "",
                "table_caption": block.get("caption", ""),
                "table_body": block.get("data", []),
                "page_idx": cnt // 10,
            }

    def parse_office_doc(self, doc_path: Union[str, Path], output_dir: Optional[str] = None, lang: Optional[str] = None, **kwargs) -> List[Dict[str, Any]]:
        try:
            doc_path = Path(doc_path)
            if not doc_path.exists(): raise FileNotFoundError
            name_without_suff = doc_path.stem
            if output_dir: base_output_dir = Path(output_dir)
            else: base_output_dir = doc_path.parent / "docling_output"
            base_output_dir.mkdir(parents=True, exist_ok=True)
            self._run_docling_command(input_path=doc_path, output_dir=base_output_dir, file_stem=name_without_suff, **kwargs)
            content_list, md_content = self._read_output_files(base_output_dir, name_without_suff)
            return {"content_list": content_list, "markdown": md_content}
        except Exception as e:
            logging.error(f"Error parse_office: {e}")
            raise

    def parse_html(self, html_path: Union[str, Path], output_dir: Optional[str] = None, lang: Optional[str] = None, **kwargs) -> List[Dict[str, Any]]:
        try:
            html_path = Path(html_path)
            if not html_path.exists(): raise FileNotFoundError
            name_without_suff = html_path.stem
            if output_dir: base_output_dir = Path(output_dir)
            else: base_output_dir = html_path.parent / "docling_output"
            base_output_dir.mkdir(parents=True, exist_ok=True)
            self._run_docling_command(input_path=html_path, output_dir=base_output_dir, file_stem=name_without_suff, **kwargs)
            content_list, md_content = self._read_output_files(base_output_dir, name_without_suff)
            return {"content_list": content_list, "markdown": md_content}
        except Exception as e:
            logging.error(f"Error parse_html: {e}")
            raise

    def check_installation(self) -> bool:
        try:
            subprocess.run(["docling", "--version"], capture_output=True, check=True)
            return True
        except Exception:
            return False
