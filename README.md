#  Package

This is a simple project package. You can find it from : https://github.com/paulchen8206/ore-reconciliation


Program created for data reconciliation report for ORE covering Canonical layer and Aurora layer:

Athena tables and view for Canonical layer topics created in production;

Python program created locally to update Glue metadata and retrieve the tables counts from Athena and Aurora databases;

An AWS SNS topic created in dev to accept the data reconciliation report, and to send it to subscribers in real-time;

The whole data reconciliation process scheduled to run daily at 7:00pm local time in Phoenix;

The manual steps include: config the AWS credentials, then kick off the reconcile program;

The future feature : automate everything.
