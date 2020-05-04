#Gus White
#This script can be used in the terminal to approve/reject assignments, designate qualifications, and distribute bonus payments on MTurk for a given batch  
#Last Updated: May 4, 2020

#INSTRUCTIONS 
#	1. Navigate to the batch you wish to process on MTurk's batch results page (filter: Submitted)
#	2. Download the CSV file of the submitted HITs to whatever folder this script is saved to as "TAC_batch_results_April30.csv" (adjust the date)
#	3. Make sure the list of variables at the top of this file (between the import calls and the call to "try") reflect the parameters of that day's batch and save the file 
#	4. In the terminal, change your directory (cd /.../...) to whatever folder this script and the .csv you downloaded are saved 
#	5. Test to ensure that the code will execute properly by typing in to the terminal: python3.6 mturk_full_automated.py test
#		5a. If someone entered an invalid completion code, this script will prompt you to change it using the command line in the terminal (to a screener cc, full survey cc, or REJECT). 
#	6. Inspect the summary results that are printed to the terminal and confirm they are correct. Make any needed adjustments. 
#	7. Execute the file and disperse payments/approvals/quals with the command: python3.6 mturk_full_automated.py pay. Confirm that the changes went through on MTurk and on the results page where you downloaded
#	   the csv, click "approve all" under submitted (even if none are present). This command also generated a new .csv file for upload to the REDCAP tracking system.
#		7a. If someone entered an invalid completion code, this script will prompt you to change it using the command line in the terminal (to a screener cc, full survey cc, or REJECT). 
#	8. Navigate to REDCAP's import tool and upload the .csv file you generated. Make sure everything looks correct. Submit.
#	9. Update the TAC_BATCH_DETAILS excel file in the R drive to reflect the batch you just finished
#	10. All done, congrats! 

#Remaining issues
# 2. Write some code that automatically grabs the results csv file from MTurk 

#import needed functions and libraries
import sys
import boto3
import pandas as pd 
import csv

#Assign variables specific to the current batch. THIS SHOULD CHANGE WITH EACH BATCH!!!!!!!! Everything else should stay the same. 
batch_results_file = #**********
screener_code = #*******
full_surv_completion_codes = #*******[]
qual_type = #*********** #This is the qualification type ID (NOT THE NAME)
qual_name = #********
date = #********* #It's important to keep the format MM/DD/YYYY HH:MM (in military time)
previous_record_id = #***** #set this value equal to the record ID of the last submitted record in redcap for study (TAC starts at 1, TMHP starts at 5000)
who_paid = #****  #Change value to correspond to your initial: MS = 0, GW = 1, RLS = 2 MU = 3, CH = 4, CC = 5, AB = 6 
which_study = #****** #Change the value to correspond to the correct study: TAC = 1, morgan no ami = 2, morgan ami = 3 

#These values should generally not change unless they need to be adjusted for a particular reason 
redcap_fields = ['record_id', 'date', 'who_paid', 'which_study', 'batch_label', 'mturk_work_id', 'screener', 'assignment_id', 'survey_completion_code', 'bonus', 'non_completer_qual', 'notes', 'payment_tracking_complete'] 
payment_tracking_complete = #***** #This sets the value of for "tracking" to complete. No need to change. 
non_completer_qual = #**** #This designates that everyone got the appropriate qualification to not repeat the study. No need to change. 
notes = #***
redcap_upload_file = #************

#This block of code sets up a flag system in the terminal so that we can first test the code to make sure it will pay the correct workers, then actually pay those workers
#To TEST the code, in the terminal enter: python3.6 mturkbonuspayer.py test ... this will display the number of turkers that will be appproved/rejected, assigned quals, and paid bonuses (and their workerIDs)
#To actually PAY workers for the HIT, in the terminal enter: python3.6 mturkbonuspayer.py pay ... this will actually execute all of the functions on MTurk and generate a CSV file to upload to REDCAP tracking 
try:
	run_mode = sys.argv[1].lower()
except:
	print("\nYou have to run this program like 'python3.6 mturkbonuspayer.py test' OR 'python3.6 mturkbonuspayer.py pay'\n")
	sys.exit()

if run_mode.lower() != "test" and run_mode.lower() != "pay":
	print("\nYou have to run this program like 'python3.6 mturkbonuspayer.py test' OR 'python3.6 mturkbonuspayer.py pay'\n")
	sys.exit()

run_mode = run_mode.lower()

#Assign variables to the values needed to access AWS and MTurk in the later boto3 call... DONT NOT CHANGE THIS UNLESS YOU HAVE YOUR OWN CREDENTIALS
region_name = 'us-east-1'
aws_access_key_id = #******
aws_secret_access_key = #********
endpoint_url = 'https://mturk-requester.us-east-1.amazonaws.com'

#Access AWS and MTurk profile 
client = boto3.client(
    'mturk',
    endpoint_url=endpoint_url,
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

#read in the .csv file of the sorted (APPROVED vs REJECTED) batch results and generates a data frame of the needed values 
df = pd.read_csv(batch_results_file)
df = df[["HITId", "WorkerId", "AssignmentId", "Answer.screenercode"]]


#determine what observations are a new assignment submission (important for writing the redcap csv file)
workers_to_add = []
total_num_workers = []
for index, row in df.iterrows():
	total_num_workers.append(row["WorkerId"])
	new_submission = client.get_assignment(
		AssignmentId = row["AssignmentId"]
	)
	if new_submission["Assignment"]["AssignmentStatus"] == "Submitted": 
		workers_to_add.append(row["WorkerId"])


#This block of code will go through the batch results and approve the assignments. If a worker submits an invalid code
#you will be prompted on the command line to change the code to an appropriate one or reject the HIT. There is also logic to ensure
# that only NEW submission within a batch will be approved (prevents client call from calling an error) 
workers_to_approve = []
test_workers_to_approve = []
workers_to_reject = []
test_workers_to_reject = []
change_comp_code = ""
new_rejects = 0 
new_approves = 0 
for index, row in df.iterrows(): 
	response_assign_status = client.get_assignment(
		AssignmentId = row["AssignmentId"]
	)
	if row["Answer.screenercode"] != screener_code and row["Answer.screenercode"] not in full_surv_completion_codes:
		change_comp_code = input("\nThe worker {} submitted an invalid completion code ({}). What should the code be changed to (screener code, full survey code, or 'REJECT'): ".format(row["WorkerId"], row["Answer.screenercode"]))
		if change_comp_code != "REJECT" and change_comp_code != screener_code and change_comp_code not in full_surv_completion_codes:
			while change_comp_code != "REJECT" and change_comp_code != screener_code and change_comp_code not in full_surv_completion_codes:
				change_comp_code = input("\nYou must enter either the screener code, a full completion code, or 'REJECT' for worker {}: ".format(row["WorkerId"]))
		df.at[index, 'Answer.screenercode'] = change_comp_code
		if change_comp_code == "REJECT":
			workers_to_reject.append(row["WorkerId"])
			if run_mode == "pay" and response_assign_status["Assignment"]["AssignmentStatus"] == "Submitted":
				reject_response = client.reject_assignment(
					AssignmentId = row["AssignmentId"],
					RequesterFeedback = "An invalid completion code was submitted on MTurk"
				)
				response_reject = client.get_assignment(
					AssignmentId = row["AssignmentId"]
				)				
				if response_reject["Assignment"]["AssignmentStatus"] == "Rejected": 
					new_rejects = new_rejects + 1 
			if row["WorkerId"] in workers_to_add:
				test_workers_to_reject.append(row["WorkerID"])
	if row["Answer.screenercode"] == screener_code or row["Answer.screenercode"] in full_surv_completion_codes:
		if run_mode == "pay" and response_assign_status["Assignment"]["AssignmentStatus"] == "Submitted":
			approve_response = client.approve_assignment(
				AssignmentId = row["AssignmentId"],
				RequesterFeedback ='TAC Survey HIT has been approved (worth $0.05). If you submitted the full survey completion code, you will be recieving a bonus payment of $2.00.',
			)
			response_approve = client.get_assignment(
				AssignmentId = row["AssignmentId"]
			)
			if response_approve["Assignment"]["AssignmentStatus"] == "Approved": 
				new_approves = new_approves + 1
		workers_to_approve.append(row["WorkerId"])
		if row["WorkerId"] in workers_to_add:
			test_workers_to_approve.append(row["WorkerID"])

#This block of code goes through everyone that submitted the HIT and assigns them the appropriate qualification to 
#prevent them from taking the survey again 
workers_to_qualify = []
test_workers_to_qualify = []
for index, row in df.iterrows(): 
	if run_mode == "pay":
		qual_response = client.associate_qualification_with_worker(
				QualificationTypeId = qual_type, 
				WorkerId = row["WorkerId"], 
				IntegerValue = 1, 
				SendNotification = False
			)
	workers_to_qualify.append(row["WorkerId"])
	if row["WorkerId"] in workers_to_add:
			test_workers_to_qualify.append(row["WorkerID"])

#This for-loop will loop through each observation in the .csv, assign num_results to hold the values for number of previous bonus payment on assignment
#then if they have NOT been bonused before AND the completion code they entered is in the list, full_surv_completion_codes, executes send_bonus function.
workers_to_pay = []
test_workers_to_pay = []
new_bonus = 0 
for index, row in df.iterrows():
	num_results = client.list_bonus_payments(AssignmentId = row["AssignmentId"])["NumResults"]
	if num_results == 0 and row["Answer.screenercode"] in full_surv_completion_codes:
		if run_mode == "pay":
			bonus_response = client.send_bonus(
				WorkerId = row["WorkerId"], 
				BonusAmount = "0.01", 
				AssignmentId = row["AssignmentId"], 
				Reason = "TAC survey completion bonus payment"
			)
			if client.list_bonus_payments(AssignmentId = row["AssignmentId"])["NumResults"] == 1: 
				new_bonus = new_bonus + 1 
	if row["Answer.screenercode"] in full_surv_completion_codes:
		workers_to_pay.append(row["WorkerId"])
		if row["WorkerId"] in workers_to_add:
			test_workers_to_pay.append(row["WorkerID"])

#if set to test flag, this will print out the # of workers that will be approved/qualified/bonused as well as the workerIDs of those that will be bonused to check before actually sending payment
if run_mode == "test":
	print("\n")
	print("\nSUMMARY OUTPUT OF PROGRAM RESULTS (TEST FLAG)\n")
	print("There were {} new workers for this version of the batch ({} total workers have completed this batch)".format(len(workers_to_add), len(total_num_workers)))
	print("If you run this with the 'pay' flag, you'd approve a TOTAL of {} assignments for this batch ({} new assignments would be approved).".format(len(workers_to_approve), len(test_workers_to_approve)))
	print("If you run this with the 'pay' flag, you'd reject a TOTAL of {} assignments for this batch ({} new assignments would be rejected).".format(len(workers_to_reject), len(test_workers_to_reject)))
	print("If you run this with the 'pay' flag, you'd assign the qualification '{}' to a TOTAL of {} workers ({} new qualifications would be assigned).".format(qual_name, len(workers_to_qualify), len(test_workers_to_qualify)))
	print("If you run this with the 'pay' flag, you'd pay bonuses to a TOTAL of {} workers for this batch ({} new bonuses would be paid)".format(len(workers_to_pay), len(test_workers_to_pay)))
	print("See below for full details on which new workers will recieve bonuses.")
	for i in test_workers_to_pay:
		print(i)
	print("\n")
	print("We currently have ${} in our account.".format(client.get_account_balance()["AvailableBalance"]))
	print("\n")


#This for loop goes through the .csv data file and prints out the full information very every observation (screeners + full completers) regarding their
#bonus history and various parameters about the assignment. Insepcting NumResults should  =1 for all full completers, and = 0 for screeners-only.
#It will also print out the number of workers paid a bonus and their workerIDs 
if run_mode == "pay":
	print("\n")
	print("\nSUMMARY OUTPUT OF PROGRAM RESULTS (PAY FLAG)\n")
	print("There were {} new workers for this version of the batch ({} total workers have completed this batch)".format(len(workers_to_add), len(total_num_workers)))
	print("This program has approved {} new assignments for this batch (a total of {} assignments have been approved for this batch).".format(new_approves, len(workers_to_approve)))
	print("This program has rejected {} new assignments for this batch (a total of {} assignments have been rejected for this batch).".format(new_rejects, len(workers_to_reject)))
	print("This program has assigned the qualification '{}' to {} new workers (a total of {} workers have recieved the qualification).".format(qual_name, len(test_workers_to_qualify), len(workers_to_qualify)))
	print("This program has paid a $2.00 bonus to the following {} new workers on MTurk (a total of {} bonuses have been paid for this batch).".format(new_bonus, len(workers_to_pay)))
	print("See below for full details on which new workers have received bonuses.")
	for i in test_workers_to_pay:
		print(i)
	print("\n")
	print("We now have ${} remaining in our account.".format(client.get_account_balance()["AvailableBalance"]))
	print("\n")

#Generate the .csv file for upload to REDCAP tracking in the test condition 
if run_mode == "test": 
	with open('REDCAP_Tracking_Upload.csv', 'w', newline='') as csvfile: 
		fieldnames = redcap_fields
		writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
		writer.writeheader()
		count = 1
		for index, row in df.iterrows():
			if row["WorkerId"] in workers_to_add: 
				record_num = previous_record_id + count
				if row["Answer.screenercode"] == screener_code or row["Answer.screenercode"] in full_surv_completion_codes:
					screener = 1
				else:
					screener = 0 
				if row["WorkerId"] in workers_to_pay:						
					bonus_paid = 1
				else: 
					bonus_paid = 0
				writer.writerow({'record_id': record_num, 'date': date, 'who_paid': who_paid, 'which_study': which_study, 'batch_label': row["HITId"], 'mturk_work_id': row["WorkerId"], 'screener': screener, 'assignment_id': row["AssignmentId"], 'survey_completion_code': row["Answer.screenercode"], 'bonus': bonus_paid, 'non_completer_qual': non_completer_qual, 'notes': notes, 'payment_tracking_complete': payment_tracking_complete})
				count = count + 1
	print("{} has been created and saved in the designated folder.\n".format(redcap_upload_file))

#Generate the .csv file for upload to REDCAP tracking in the pay condition 
if run_mode == "pay": 
	with open('REDCAP_Tracking_Upload.csv', 'w', newline='') as csvfile: 
		fieldnames = redcap_fields
		writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
		writer.writeheader()
		count = 1
		for index, row in df.iterrows():
			if row["WorkerId"] in workers_to_add: 
				record_num = previous_record_id + count
				if row["Answer.screenercode"] == screener_code or row["Answer.screenercode"] in full_surv_completion_codes:
					screener = 1
				else:
					screener = 0 
				if row["WorkerId"] in workers_to_pay:						
					bonus_paid = 1
				else: 
					bonus_paid = 0
				writer.writerow({'record_id': record_num, 'date': date, 'who_paid': who_paid, 'which_study': which_study, 'batch_label': row["HITId"], 'mturk_work_id': row["WorkerId"], 'screener': screener, 'assignment_id': row["AssignmentId"], 'survey_completion_code': row["Answer.screenercode"], 'bonus': bonus_paid, 'non_completer_qual': non_completer_qual, 'notes': notes, 'payment_tracking_complete': payment_tracking_complete})
				count = count + 1
	print("{} has been created and saved in the designated folder.\n".format(redcap_upload_file))


#END CODE