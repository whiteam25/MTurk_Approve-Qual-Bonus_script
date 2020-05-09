import sys
import boto3
import pandas as pd 
import csv

#Assign variables specific to the current batch. THIS SHOULD CHANGE WITH EACH BATCH!!!!!!!! Everything else should stay the same. 
batch_results_file = "******"
hitid = "*********"

#Assign variables to the values needed to access AWS and MTurk in the later boto3 call... DONT NOT CHANGE THIS UNLESS YOU HAVE YOUR OWN CREDENTIALS
region_name = '*******'
aws_access_key_id = '********'
aws_secret_access_key = '********'
endpoint_url = '*******'

#Access AWS and MTurk profile 
client = boto3.client(
    'mturk',
    endpoint_url=endpoint_url,
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

df = pd.read_csv(batch_results_file)
df = df[["hitid", "workerid", "assignmentid", "answerscreenercode"]]

all_batches_output_file = open("all_batches_output_file.txt", "w+")

total_approved = 0 
total_rejected = 0 
total_bonuses_num = 0
total_bonuses_amount = 0
num_people_bonused = 0 
total_to_workers = 0
total_to_mturk = 0 
total_cost = 0 

print("\nHIT PROCESSING CHECK (ALL BATCHES) -- OUTPUT\n")
all_batches_output_file.write("\nHIT PROCESSING CHECK (ALL BATCHES) -- OUTPUT\n")
all_batches_output_file.write("\n")

for index, row in df.iterrows():
	paid_to_worker = 0
	paid_to_mturk = 0 

	num_bonuses = client.list_bonus_payments(AssignmentId = row["assignmentid"])["NumResults"]
	if num_bonuses > 0: num_people_bonused = num_people_bonused + 1 
	amount_bonuses = 0 

	for i in range(0, num_bonuses): 
		bonus_int = float(client.list_bonus_payments(AssignmentId = row["assignmentid"])["BonusPayments"][i]["BonusAmount"]) 
		amount_bonuses = bonus_int + amount_bonuses
		total_bonuses_num = total_bonuses_num + 1 
		total_bonuses_amount = total_bonuses_amount + bonus_int

	assign_status = client.get_assignment(AssignmentId = row["assignmentid"])
	if assign_status["Assignment"]["AssignmentStatus"] == "Approved": total_approved = total_approved + 1 
	if assign_status["Assignment"]["AssignmentStatus"] == "Rejected": total_rejected = total_rejected + 1 

	print("Worker ID: {}".format(row["workerid"]))
	all_batches_output_file.write("Worker ID: {}\n".format(row["workerid"]))

	print("HIT: {} (ID #{})".format(assign_status["HIT"]["Title"], row["hitid"]))
	all_batches_output_file.write("HIT ID: {}\n".format(row["hitid"]))
	
	print("Assignment ID: {}".format(row["assignmentid"]))
	all_batches_output_file.write("Assignment ID: {}\n".format(row["assignmentid"]))

	print("Assignment status: {}".format(assign_status["Assignment"]["AssignmentStatus"]))
	all_batches_output_file.write("Assignment status: {}\n".format(assign_status["Assignment"]["AssignmentStatus"]))

	if assign_status["Assignment"]["AssignmentStatus"] == "Approved": 
		print("Assignment approval time: {}".format(assign_status["Assignment"]["ApprovalTime"]))
		all_batches_output_file.write("Assignment approval time: {}\n".format(assign_status["Assignment"]["ApprovalTime"]))

		print("Assignment reward: ${}".format(assign_status["HIT"]["Reward"]))
		all_batches_output_file.write("Assignment reward: ${}\n".format(assign_status["HIT"]["Reward"]))
		int_reward = float(assign_status["HIT"]["Reward"])

	if assign_status["Assignment"]["AssignmentStatus"] == "Rejected": 
		print("Assignment approval time: {}".format(assign_status["Assignment"]["RejectionTime"]))
		all_batches_output_file.write("Assignment approval time: {}\n".format(assign_status["Assignment"]["RejectionTime"]))

		print("Assignment reward: $0.00")
		all_batches_output_file.write("Assignment reward: $0.00\n")

	print("Number of bonuses paid: {}".format(num_bonuses))
	all_batches_output_file.write("Number of bonuses paid: {}\n".format(num_bonuses))

	print("Bonus amount: ${:0.2f}".format(amount_bonuses))
	all_batches_output_file.write("Bonus amount: ${:0.2f}\n".format(amount_bonuses))

	paid_to_worker = int_reward + amount_bonuses
	paid_to_mturk = (int_reward * 0.4) + (amount_bonuses * 0.2)

	total_to_workers = total_to_workers + paid_to_worker
	total_to_mturk = total_to_mturk + paid_to_mturk
	total_worker_expenditure = paid_to_worker + paid_to_mturk

	print("Total expenditure (rewards, bonuses, fees): ${:0.2f}".format(total_worker_expenditure))
	all_batches_output_file.write("Total expenditure (rewards, bonuses, fees): ${:0.2f}".format(total_worker_expenditure))

	print("\n")
	all_batches_output_file.write("\n")
	all_batches_output_file.write("\n")

total_cost = total_to_workers + total_to_mturk

print("For HIT {}, {} assignments were approved, {} were rejected, and {} bonuses were paid to {} people".format(hitid, total_approved, total_rejected, total_bonuses_num, num_people_bonused))
print("Total spent: ${:0.2f} (${:0.2f} to workers and ${:0.2f} in fees to MTurk)\n".format(total_cost, total_to_workers, total_to_mturk))
all_batches_output_file.write("For HIT {}, {} assignments were approved, {} were rejected, and {} bonuses were paid to {} people.\n".format(hitid, total_approved, total_rejected, total_bonuses_num, num_people_bonused))
all_batches_output_file.write("Total spent: ${:0.2f} (${:0.2f} to workers and ${:0.2f} in fees to MTurk.\n)".format(total_cost, total_to_workers, total_to_mturk))
print("\n")
all_batches_output_file.close()

#END CODE