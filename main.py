import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
import logging
import os
import time
import zipfile
import threading
import queue
from pathlib import Path
from datetime import datetime
from playwright.sync_api import sync_playwright, Error as PlaywrightError
import pandas as pd

# --- Setup Logging ---
logging.basicConfig(filename='epfo_scraper.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# --- Thread-safe queues for communication between GUI and Playwright thread ---
command_queue = queue.Queue()
result_queue = queue.Queue()


# --- This is the dedicated thread for ALL Playwright operations ---
def playwright_worker():
    """
    This function runs in a separate, long-running thread.
    It initializes Playwright and waits for commands from the command_queue.
    """
    playwright_context = {}

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()
            playwright_context['page'] = page
            playwright_context['browser'] = browser
            result_queue.put(('status_update', "Ready. Please log in to begin."))
        except PlaywrightError as e:
            result_queue.put(('error', f"Could not launch browser: {e}"))
            return

        while True:
            try:
                command, data = command_queue.get()

                if command == 'shutdown':
                    result_queue.put(('status_update', "Shutting down..."))
                    break

                elif command == 'open_login_page':
                    try:
                        page.goto(
                            "https://unifiedportal-emp.epfindia.gov.in/epfo/",
                            timeout=200000,
                            wait_until="domcontentloaded"
                        )
                        result_queue.put(('browser_opened', None))
                    except PlaywrightError as e:
                        result_queue.put(('error', f"Could not navigate: {e}"))
                
                elif command == 'verify_login':
                    try:
                        page.wait_for_selector('a:has-text("Member")', timeout=5000)
                        result_queue.put(('login_verified', True))
                    except PlaywrightError:
                        result_queue.put(('login_verified', False))
                
                elif command == 'run_uan':
                    run_uan_extraction(page, data)
                
                elif command == 'run_ecr':
                    run_ecr_extraction(page, data)
                
                # --- NEW TASK ADDED HERE ---
                elif command == 'run_msd':
                    run_msd_extraction(page, data)

            except Exception as e:
                result_queue.put(('error', f"An unexpected error occurred in the worker thread: {e}"))

    if playwright_context.get('browser'):
        playwright_context['browser'].close()


# --- UI Functions (These run in the main thread) ---
def process_result_queue():
    """Checks the result queue for messages from the worker and updates the GUI."""
    try:
        result_type, data = result_queue.get_nowait()
        
        if result_type == 'status_update':
            update_status(data)
        elif result_type == 'error':
            messagebox.showerror("Error", data)
            update_status("Error occurred. Ready for new task.")
        elif result_type == 'info':
            messagebox.showinfo("Info", data)
        elif result_type == 'browser_opened':
            update_ui_state('waiting_for_verify')
            update_status("Browser open. Please log in manually on the website.")
        elif result_type == 'login_verified':
            if data:
                messagebox.showinfo("Success", "Login verified! You can now use the extraction tools.")
                update_ui_state('logged_in')
                update_status("Logged In. Ready for tasks.")
            else:
                messagebox.showwarning("Verification Failed", "Login not detected. Please ensure you are fully logged in on the website and then click 'Verify Login Status' again.")
                update_status("Login not verified. Please log in on the website.")
                
    except queue.Empty:
        pass
    finally:
        root.after(100, process_result_queue)


def update_ui_state(state_name):
    """
    Manages the GUI state. States: 'initial', 'waiting_for_verify', 'logged_in'.
    """
    task_state = tk.DISABLED
    if state_name == 'initial':
        task_state = tk.DISABLED
        login_button.pack(pady=10)
        verify_button.pack_forget()
        logout_button.pack_forget()
    elif state_name == 'waiting_for_verify':
        task_state = tk.DISABLED
        login_button.pack_forget()
        verify_button.pack(pady=10)
        logout_button.pack(pady=10)
    elif state_name == 'logged_in':
        task_state = tk.NORMAL
        login_button.pack_forget()
        verify_button.pack_forget()
        logout_button.pack(pady=10)

    # --- UPDATED: Added the new frame to the state update logic ---
    for frame in [uan_frame, ecr_frame, msd_frame]:
        for child in frame.winfo_children():
            try:
                child.configure(state=task_state)
            except tk.TclError:
                pass

def browse_file():
    """Opens a file dialog to select the output file for UAN data."""
    filename = filedialog.asksaveasfilename(
        initialfile="epfo_data.xlsx",
        defaultextension=".xlsx",
        filetypes=[("Excel Files", "*.xlsx"), ("All Files", "*.*")]
    )
    if filename:
        output_file_entry.delete(0, tk.END)
        output_file_entry.insert(0, filename)

# --- Button Command Handlers (These put commands on the queue) ---
def handle_open_browser():
    update_status("Opening browser...")
    command_queue.put(('open_login_page', None))

def handle_verify_login():
    update_status("Verifying login status...")
    command_queue.put(('verify_login', None))

def handle_logout():
    update_status("Logging out and closing browser...")
    command_queue.put(('shutdown', None))
    root.after(500, start_worker_thread)
    root.after(600, lambda: update_ui_state('initial'))


# --- Task Execution Functions (Now called by the worker thread) ---
def get_month_index(month_str):
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    try:
        return months.index(month_str.title()) + 1
    except ValueError:
        return -1
        
def run_uan_extraction(page, data):
    # This function is unchanged
    uans = data['uans']
    output_file = data['output_file']
    result_queue.put(('status_update', "Starting UAN extraction..."))

    try:
        page.click('a:has-text("Member")')
        page.click('a:has-text("Member Profile")')
        page.wait_for_selector("#memberList", timeout=60000)
    except PlaywrightError as e:
        result_queue.put(('error', f"Could not navigate to 'Member Profile': {e}"))
        return

    all_uan_data = []
    for uan in uans:
        result_queue.put(('status_update', f"Extracting data for UAN: {uan}..."))
        try:
            search_input = page.locator('input[type="search"][aria-controls="memberList"]')
            search_input.fill(uan); search_input.press('Enter')
            page.wait_for_timeout(2000)
            row = page.locator("#memberList tbody tr:first-child")
            name = row.locator("td:nth-child(2)").inner_text().strip()
            joining_date = row.locator("td:nth-child(6)").inner_text().strip()
            exit_date = row.locator("td:nth-child(7)").inner_text().strip()
            all_uan_data.append({"UAN": uan, "Name": name, "Joining Date": joining_date, "Exit Date": exit_date})
        except PlaywrightError as e:
            logging.error(f"Could not extract all data for UAN {uan}: {e}")
    
    if all_uan_data:
        df = pd.DataFrame(all_uan_data); df.to_excel(output_file, index=False)
        result_queue.put(('info', f"UAN data extracted and saved to {output_file}"))
    else:
        result_queue.put(('info', "No UAN data was extracted."))
    result_queue.put(('status_update', "UAN extraction finished."))

def run_ecr_extraction(page, data):
    # This function is unchanged
    start_date = data['start_date']; end_date = data['end_date']
    result_queue.put(('status_update', "Starting ECR PDF extraction..."))

    try:
        page.click('a:has-text("Payments")'); page.click('a:has-text("Payment (ECR)")')
        page.wait_for_selector('a:has-text("ECR Upload")', timeout=200000)
        page.click('a:has-text("ECR Upload")')
        page.wait_for_selector('table#tbRecentClaimList', timeout=200000)
    except PlaywrightError as e:
        result_queue.put(('error', f"Could not navigate to ECR page: {e}"))
        return
        
    download_dir = Path("ecr_downloads"); download_dir.mkdir(exist_ok=True)
    downloaded_files = []

    while True:
        page.wait_for_timeout(20000)
        rows = page.locator("table#tbRecentClaimList tbody tr").all()
        for row in rows:
            try:
                wage_month_str = row.locator("td:nth-child(3)").inner_text()
                status = row.locator("td:nth-child(8)").inner_text().strip()
                if status == "Payment Confirmed":
                    month_str, year_str = wage_month_str.split('-')
                    wage_date = datetime(int(year_str), get_month_index(month_str), 1)
                    if start_date <= wage_date <= end_date:
                        trrn = row.locator("td:nth-child(2)").inner_text().strip()
                        result_queue.put(('status_update', f"Downloading PDF for {trrn}..."))
                        pdf_link = row.locator('td:nth-child(10) a')
                        if pdf_link.count() > 0:
                            with page.expect_download() as download_info:
                                pdf_link.click()
                            download = download_info.value
                            file_path = download_dir / f"{trrn}_{wage_month_str}.pdf"
                            download.save_as(file_path); downloaded_files.append(file_path)
            except (PlaywrightError, ValueError) as e:
                logging.error(f"Error processing a row: {e}")
        next_button = page.locator('a:has-text("Next")')
        if not next_button.is_visible(): break
        next_button.click()

    if downloaded_files:
        zip_filename = f"ECR_Statements_{start_date.strftime('%Y%m')}_to_{end_date.strftime('%Y%m')}.zip"
        with zipfile.ZipFile(zip_filename, 'w') as zf:
            for f in downloaded_files: zf.write(f, f.name); os.remove(f)
        result_queue.put(('info', f"ECR PDFs zipped to {zip_filename}"))
    else:
        result_queue.put(('info', "No matching ECR statements found."))
    result_queue.put(('status_update', "ECR extraction finished."))

# --- NEW FUNCTION FOR TASK 3 ---
def run_msd_extraction(page, data):
    """Navigates to Member Service Details, searches by UAN, and saves tables to Excel files."""
    uans = data['uans']
    result_queue.put(('status_update', "Starting Member Service Detail extraction..."))
    
    # Create a temporary directory for the Excel files
    excel_dir = Path("msd_excel_files")
    excel_dir.mkdir(exist_ok=True)
    generated_files = []

    try:
        # Navigate to the correct page once
        result_queue.put(('status_update', "Navigating to Member Service Details page..."))
        page.click('a:has-text("Dashboards")')
        page.click('a:has-text("MEMBER SERVICE DETAILS")')
        page.wait_for_selector('input#uanNo', timeout=60000)

        for uan in uans:
            result_queue.put(('status_update', f"Processing UAN: {uan}"))
            page.fill('input#uanNo', uan)
            page.click('button:has-text("Search")')
            
            # Wait for the table grid to reload after search
            page.wait_for_selector('#load_profileService', state='hidden', timeout=60000)
            page.wait_for_timeout(1000) # Small delay for safety

            # Scrape headers
            headers = page.locator(".ui-jqgrid-htable .ui-jqgrid-labels th").all_inner_texts()
            # The first column is a blank number column, so we can skip it.
            headers = [h.strip() for h in headers if h.strip()][1:] 

            all_rows_data = []
            
            # Handle pagination
            while True:
                # Scrape rows from the current page
                rows = page.locator("table#profileService tbody tr.jqgrow").all()
                if not rows and not all_rows_data: # Check if member not found on first page
                    if "Member not found" in page.locator("#profileServicePager_right").inner_text():
                        logging.warning(f"Member not found for UAN: {uan}")
                        break
                
                for row in rows:
                    cells = row.locator("td").all()
                    # Skip the first cell (row number)
                    row_data = [cell.inner_text() for cell in cells[1:]]
                    all_rows_data.append(row_data)

                # Check for next page
                next_button = page.locator("#next_profileServicePager")
                if "ui-state-disabled" in (next_button.get_attribute("class") or ""):
                    break
                else:
                    result_queue.put(('status_update', f"UAN {uan}: Found multiple pages, going to next page..."))
                    next_button.click()
                    page.wait_for_selector('#load_profileService', state='hidden', timeout=60000)

            # Save data for the current UAN to an Excel file
            if all_rows_data:
                df = pd.DataFrame(all_rows_data, columns=headers)
                excel_path = excel_dir / f"{uan}.xlsx"
                df.to_excel(excel_path, index=False)
                generated_files.append(excel_path)
                logging.info(f"Saved service details for UAN {uan} to {excel_path}")

    except PlaywrightError as e:
        result_queue.put(('error', f"An error occurred during MSD extraction: {e}"))
        return

    # Zip all generated excel files
    if generated_files:
        zip_filename = "Member_Service_Details.zip"
        result_queue.put(('status_update', f"Zipping {len(generated_files)} Excel files..."))
        with zipfile.ZipFile(zip_filename, 'w') as zf:
            for f in generated_files:
                zf.write(f, f.name)
                os.remove(f) # Clean up individual file
        
        # Clean up the temporary directory
        if excel_dir.exists():
             os.rmdir(excel_dir)
        
        result_queue.put(('info', f"Task complete. All service details saved to {zip_filename}"))
    else:
        result_queue.put(('info', "No data was extracted or saved."))
    
    result_queue.put(('status_update', "Member Service Detail extraction finished."))


# --- GUI Setup ---
root = tk.Tk()
root.title("EPFO Data Extractor")
root.geometry("600x750") # Increased height for the new task

main_frame = ttk.Frame(root, padding="10")
main_frame.pack(fill=tk.BOTH, expand=True)

# --- Section 1: Login Management ---
login_frame = ttk.LabelFrame(main_frame, text="Step 1: Login Control", padding="10")
login_frame.pack(fill=tk.X, expand=True, pady=5)
login_button = ttk.Button(login_frame, text="Open Browser for Manual Login", command=handle_open_browser)
verify_button = ttk.Button(login_frame, text="Verify Login Status", command=handle_verify_login)
logout_button = ttk.Button(login_frame, text="Logout & Close Browser", command=handle_logout)

# --- Section 2: UAN Data Extraction ---
def uan_button_command():
    uans = [u.strip() for u in uans_entry.get("1.0", tk.END).split(',') if u.strip()]
    output_file = output_file_entry.get()
    if not uans or not output_file:
        messagebox.showerror("Input Error", "Please provide UANs and an output file path.")
        return
    command_queue.put(('run_uan', {'uans': uans, 'output_file': output_file}))

uan_frame = ttk.LabelFrame(main_frame, text="Task 1: UAN Profile Extractor", padding="10")
uan_frame.pack(fill=tk.X, expand=True, pady=5)
uan_frame.columnconfigure(1, weight=1)
ttk.Label(uan_frame, text="UANs (comma-separated):").grid(row=0, column=0, padx=5, pady=5, sticky="nw")
uans_entry = scrolledtext.ScrolledText(uan_frame, width=40, height=3)
uans_entry.grid(row=0, column=1, columnspan=2, padx=5, pady=5, sticky="ew")
ttk.Label(uan_frame, text="Output Excel File:").grid(row=1, column=0, padx=5, pady=5, sticky="w")
output_file_entry = ttk.Entry(uan_frame, width=30)
output_file_entry.grid(row=1, column=1, padx=5, pady=5, sticky="ew")
output_file_entry.insert(0, "epfo_data.xlsx")
browse_button = ttk.Button(uan_frame, text="Browse", command=browse_file)
browse_button.grid(row=1, column=2, padx=5, pady=5)
run_uan_button = ttk.Button(uan_frame, text="Run UAN Extraction", command=uan_button_command)
run_uan_button.grid(row=2, column=1, padx=5, pady=10)

# --- Section 3: ECR PDF Extraction ---
def ecr_button_command():
    try:
        start_date = datetime(int(start_year_entry.get()), month_map[start_month_var.get()], 1)
        end_date = datetime(int(end_year_entry.get()), month_map[end_month_var.get()], 1)
        command_queue.put(('run_ecr', {'start_date': start_date, 'end_date': end_date}))
    except (ValueError, KeyError):
        messagebox.showerror("Input Error", "Please provide a valid date range.")

ecr_frame = ttk.LabelFrame(main_frame, text="Task 2: Download ECR Statement PDFs", padding="10")
ecr_frame.pack(fill=tk.X, expand=True, pady=5)
months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
month_map = {name: i+1 for i, name in enumerate(months)}
date_frame = ttk.Frame(ecr_frame)
date_frame.pack(pady=5)
ttk.Label(date_frame, text="Start Date:").grid(row=0, column=0, padx=5, pady=5)
start_month_var = tk.StringVar(value=months[0])
start_month_menu = ttk.Combobox(date_frame, textvariable=start_month_var, values=months, width=7, state="readonly")
start_month_menu.grid(row=0, column=1, padx=5, pady=5)
start_year_entry = ttk.Entry(date_frame, width=7); start_year_entry.grid(row=0, column=2, padx=5, pady=5); start_year_entry.insert(0, datetime.now().year)
ttk.Label(date_frame, text="End Date:").grid(row=1, column=0, padx=5, pady=5)
end_month_var = tk.StringVar(value=months[datetime.now().month - 1])
end_month_menu = ttk.Combobox(date_frame, textvariable=end_month_var, values=months, width=7, state="readonly")
end_month_menu.grid(row=1, column=1, padx=5, pady=5)
end_year_entry = ttk.Entry(date_frame, width=7); end_year_entry.grid(row=1, column=2, padx=5, pady=5); end_year_entry.insert(0, datetime.now().year)
run_ecr_button = ttk.Button(ecr_frame, text="Run ECR PDF Extraction", command=ecr_button_command)
run_ecr_button.pack(pady=10)

# --- NEW SECTION FOR TASK 3 ---
def msd_button_command():
    uans = [u.strip() for u in msd_uans_entry.get("1.0", tk.END).split(',') if u.strip()]
    if not uans:
        messagebox.showerror("Input Error", "Please provide at least one UAN for Service Detail extraction.")
        return
    command_queue.put(('run_msd', {'uans': uans}))

msd_frame = ttk.LabelFrame(main_frame, text="Task 3: Member Service Details Extractor", padding="10")
msd_frame.pack(fill=tk.X, expand=True, pady=5)
msd_frame.columnconfigure(1, weight=1)
ttk.Label(msd_frame, text="UANs (comma-separated):").grid(row=0, column=0, padx=5, pady=5, sticky="nw")
msd_uans_entry = scrolledtext.ScrolledText(msd_frame, width=40, height=3)
msd_uans_entry.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
run_msd_button = ttk.Button(msd_frame, text="Run Service Detail Extraction", command=msd_button_command)
run_msd_button.grid(row=1, column=1, padx=5, pady=10)

# --- Status Bar ---
status_var = tk.StringVar()
status_bar = ttk.Label(root, textvariable=status_var, relief=tk.SUNKEN, anchor=tk.W, padding=5)
status_bar.pack(side=tk.BOTTOM, fill=tk.X)
def update_status(message):
    status_var.set(message); root.update_idletasks()

# --- Initial UI State and Final Setup ---
def start_worker_thread():
    worker = threading.Thread(target=playwright_worker, daemon=True)
    worker.start()

def on_closing():
    command_queue.put(('shutdown', None))
    root.after(500, root.destroy)

update_ui_state('initial')
update_status("Starting up... Please wait for the 'Ready' signal.")
start_worker_thread()
root.after(100, process_result_queue)
root.protocol("WM_DELETE_WINDOW", on_closing)
root.mainloop()