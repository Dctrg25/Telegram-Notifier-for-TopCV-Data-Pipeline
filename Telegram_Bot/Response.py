from datetime import datetime

def sample_response(input):
    user_message = str(input).lower()

    if user_message in ("hello", "hi", "sup"):
        return "Hey! How's it going?"
    if user_message in ("who are you", "who are you?"):
        return "I am a bot"
    if user_message in ("time", "time?"):
        now = datetime.now()
        date_time = now.strftime("%d/%m/%y, %H:%M:%S")
        return str(date_time)
    if "job" in user_message:
        return "Please enter the correct format: /job_description min_salary max_salary jobs_number"
    return "I don't understand you"