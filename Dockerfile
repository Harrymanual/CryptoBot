# Use an official Python runtime as the parent image
FROM python:3.10

# Set the working directory in the container
WORKDIR /

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# (Optional) If you have a requirements.txt, uncomment the following line:
# RUN pip install --no-cache-dir -r requirements.txt

# Command to run on container start. 
# Here you might want to run a script, for example:
# CMD ["python", "./your-main-script.py"]

# If you just want to keep the container running and execute scripts manually, use:
CMD ["tail", "-f", "/dev/null"]
