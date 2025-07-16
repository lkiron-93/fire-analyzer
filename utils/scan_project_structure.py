"""
Project Folder Structure Scanner
Generates a detailed view of your project directory structure
"""

import os
import json
from pathlib import Path
from datetime import datetime

def get_file_size(size_bytes):
    """Convert bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"

def scan_directory(path, ignore_patterns=None):
    """Recursively scan directory and build structure"""
    if ignore_patterns is None:
        ignore_patterns = [
            '__pycache__',
            '.git',
            '.env',
            'venv',
            'env',
            '.idea',
            '.vscode',
            '*.pyc',
            '.DS_Store',
            'Thumbs.db'
        ]
    
    structure = {
        'name': os.path.basename(path),
        'type': 'directory',
        'path': path,
        'children': []
    }
    
    try:
        items = sorted(os.listdir(path))
        
        for item in items:
            # Skip ignored patterns
            if any(pattern in item for pattern in ignore_patterns):
                continue
                
            item_path = os.path.join(path, item)
            
            if os.path.isdir(item_path):
                # Recursively scan subdirectory
                sub_structure = scan_directory(item_path, ignore_patterns)
                structure['children'].append(sub_structure)
            else:
                # Add file info
                try:
                    size = os.path.getsize(item_path)
                    file_info = {
                        'name': item,
                        'type': 'file',
                        'size': get_file_size(size),
                        'extension': os.path.splitext(item)[1]
                    }
                    structure['children'].append(file_info)
                except:
                    pass
    
    except PermissionError:
        structure['error'] = 'Permission denied'
    
    return structure

def print_tree(structure, prefix="", is_last=True):
    """Print directory structure as tree"""
    lines = []
    
    # Determine the connector
    if prefix == "":
        connector = ""
    else:
        connector = "└── " if is_last else "├── "
    
    # Add current item
    if structure['type'] == 'directory':
        lines.append(f"{prefix}{connector}📁 {structure['name']}/")
    else:
        lines.append(f"{prefix}{connector}📄 {structure['name']} ({structure.get('size', 'N/A')})")
    
    # Add children
    if 'children' in structure:
        # Determine new prefix
        if prefix == "":
            new_prefix = ""
        else:
            new_prefix = prefix + ("    " if is_last else "│   ")
        
        # Sort children: directories first, then files
        children = sorted(structure['children'], 
                         key=lambda x: (x['type'] != 'directory', x['name'].lower()))
        
        for i, child in enumerate(children):
            is_last_child = (i == len(children) - 1)
            lines.extend(print_tree(child, new_prefix, is_last_child))
    
    return lines

def generate_markdown_report(structure, root_path):
    """Generate a detailed markdown report"""
    lines = []
    
    # Header
    lines.append("# FIRE Analyzer Project Structure")
    lines.append(f"\n**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"**Root Path:** `{root_path}`\n")
    
    # Summary statistics
    file_count = 0
    dir_count = 0
    file_types = {}
    
    def count_items(node):
        nonlocal file_count, dir_count, file_types
        if node['type'] == 'directory':
            dir_count += 1
            for child in node.get('children', []):
                count_items(child)
        else:
            file_count += 1
            ext = node.get('extension', 'no_ext')
            file_types[ext] = file_types.get(ext, 0) + 1
    
    count_items(structure)
    
    lines.append("## Summary")
    lines.append(f"- **Total Directories:** {dir_count}")
    lines.append(f"- **Total Files:** {file_count}")
    lines.append("\n### File Types")
    for ext, count in sorted(file_types.items()):
        lines.append(f"- `{ext}`: {count} files")
    
    # Directory tree
    lines.append("\n## Directory Structure")
    lines.append("```")
    tree_lines = print_tree(structure)
    lines.extend(tree_lines)
    lines.append("```")
    
    # Detailed file listing by category
    lines.append("\n## File Categories")
    
    # Python files
    lines.append("\n### Python Files (.py)")
    py_files = []
    
    def find_files_by_extension(node, extension, path=""):
        results = []
        current_path = os.path.join(path, node['name']) if path else node['name']
        
        if node['type'] == 'file' and node.get('extension') == extension:
            results.append((current_path, node))
        elif node['type'] == 'directory':
            for child in node.get('children', []):
                results.extend(find_files_by_extension(child, extension, current_path))
        return results
    
    py_files = find_files_by_extension(structure, '.py')
    for path, file in sorted(py_files):
        lines.append(f"- `{path}` ({file.get('size', 'N/A')})")
    
    # JSON files
    lines.append("\n### JSON Files (.json)")
    json_files = find_files_by_extension(structure, '.json')
    for path, file in sorted(json_files):
        lines.append(f"- `{path}` ({file.get('size', 'N/A')})")
    
    # Text/Data files
    lines.append("\n### Data Files (.txt, .csv, .xlsx)")
    data_extensions = ['.txt', '.csv', '.xlsx']
    data_files = []
    for ext in data_extensions:
        data_files.extend(find_files_by_extension(structure, ext))
    
    for path, file in sorted(data_files):
        lines.append(f"- `{path}` ({file.get('size', 'N/A')})")
    
    return "\n".join(lines)

def main():
    # Get the project root
    project_root = r"C:\Users\lukek\OneDrive\Desktop\fire-analyzer"
    
    print("🔍 Scanning FIRE Analyzer project structure...")
    print(f"Root: {project_root}\n")
    
    # Scan directory
    structure = scan_directory(project_root)
    
    # Generate outputs
    output_dir = project_root
    
    # Save JSON structure
    json_file = os.path.join(output_dir, "project_structure.json")
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(structure, f, indent=2)
    print(f"✅ Saved JSON structure to: {json_file}")
    
    # Save Markdown report
    markdown_file = os.path.join(output_dir, "project_structure.md")
    report = generate_markdown_report(structure, project_root)
    with open(markdown_file, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"✅ Saved Markdown report to: {markdown_file}")
    
    # Print tree to console
    print("\n📁 Project Structure:")
    print("=" * 60)
    tree_lines = print_tree(structure)
    for line in tree_lines:
        print(line)
    
    print("\n✨ Done! Check the generated files for detailed information.")

if __name__ == "__main__":
    main()