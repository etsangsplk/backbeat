# Using Backbeat

## Manually setup replication

This guide walks through the steps to manually setup replication on a source
and target environment.

It assumes:

* A source environment at `http://node1`
* A target environment at `http://node6`
* An AWS profile `backbeat-source` in the region 'us-east-1'
* An AWS profile `backbeat-target` in the region 'us-east-1'
* That we want to set up a bucket `source-bucket` to replicate to another bucket
  `target-bucket`

### Create roles

Create a file backbeat-role-trust-policy.json with the following content:

```sh
{
    "Version":"2012-10-17",
    "Statement":[
        {
            "Effect":"Allow",
            "Principal":{
                "Service":"backbeat"
            },
            "Action":"sts:AssumeRole"
        }
    ]
}
```

In the next steps, note that we will need to later reference the Amazon Resource
Name (ARN) value from the output of each command.

Create the role on the source environment:

```sh
aws iam create-role \
--role-name SourceRoleForS3Replication \
--assume-role-policy-document file://backbeat-role-trust-policy.json \
--endpoint http://node1:8600 \
--profile backbeat-source
```

Output will look something like:

```sh
{
    "Role": {
        "AssumeRolePolicyDocument": {...},
        "RoleId": "...",
        "CreateDate": "...",
        "RoleName": "...",
        "Path": "...",
        "Arn": "arn:aws:iam::668546647514:role/SourceRoleForS3Replication"
    }
}
```

Create the role on the target environment:

```sh
aws iam create-role \
--role-name TargetRoleForS3Replication \
--assume-role-policy-document file://backbeat-role-trust-policy.json \
--endpoint http://node6:8600 \
--profile backbeat-target
```

Output will look something like:

```sh
{
    "Role": {
        "AssumeRolePolicyDocument": {...},
        "RoleId": "...",
        "CreateDate": "...",
        "RoleName": "...",
        "Path": "...",
        "Arn": "arn:aws:iam::779657758625:role/TargetRoleForS3Replication"
    }
}
```

Now, save each role's ARN value from the output of the commands above (the field
`Arn`). These will be needed to setup the replication configuration on a bucket
in a later step. In this example, the source role ARN is
`arn:aws:iam::668546647514:role/SourceRoleForS3Replication` and the target role
ARN is `arn:aws:iam::779657758625:role/TargetRoleForS3Replication`.

### Create and attach policies

Create a file S3-role-permissions-policy.json with the following content:

```sh
{
   "Version":"2012-10-17",
   "Statement":[
        {
            "Effect":"Allow",
            "Action":[
                "s3:GetObjectVersion",
                "s3:GetObjectVersionAcl"
            ],
            "Resource":[
                "arn:aws:s3:::source-bucket/*"
            ]
        },
        {
            "Effect":"Allow",
            "Action":[
                "s3:ListBucket",
                "s3:GetReplicationConfiguration"
            ],
            "Resource":[
                "arn:aws:s3:::source-bucket"
            ]
        },
        {
            "Effect":"Allow",
            "Action":[
                "s3:ReplicateObject",
                "s3:ReplicateDelete"
            ],
            "Resource":"arn:aws:s3:::target-bucket/*"
        }
   ]
}
```

In the next steps, note that we will need to later reference the Amazon Resource
Name (ARN) value from the output of each command.

Create the policy on the source environment:

```sh
aws iam create-policy \
--policy-name SourcePolicyForS3Replication  \
--policy-document file://S3-role-permissions-policy.json \
--endpoint http://node1:8600 \
--profile backbeat-source
```

Output will look something like:

```sh
{
    "Policy": {
        "PolicyName": "...",
        "CreateDate": "...",
        "AttachmentCount": ...,
        "IsAttachable": ...,
        "PolicyId": "...",
        "DefaultVersionId": "...",
        "Path": "...",
        "Arn": "arn:aws:iam::668546647514:policy/SourcePolicyForS3Replication",
        "UpdateDate": "..."
    }
}
```

Now, using the ARN of the policy (the field `Arn`), attach the policy to the
source role:

```sh
aws iam attach-role-policy \
--role-name SourceRoleForS3Replication \
--policy-arn arn:aws:iam::668546647514:policy/SourcePolicyForS3Replication \
--endpoint http://node1:8600 \
--profile backbeat-source
```

Create the policy on the target environment:

```sh
aws iam create-policy \
--policy-name TargetPolicyForS3Replication  \
--policy-document file://S3-role-permissions-policy.json \
--endpoint http://node6:8600 \
--profile backbeat-target
```

Output will look something like:

```sh
{
    "Policy": {
        "PolicyName": "...",
        "CreateDate": "...",
        "AttachmentCount": ...,
        "IsAttachable": ...,
        "PolicyId": "...",
        "DefaultVersionId": "...",
        "Path": "...",
        "Arn": "arn:aws:iam::779657758625:policy/TargetPolicyForS3Replication",
        "UpdateDate": "..."
    }
}
```

Now, using the ARN of the policy (the field `Arn`), attach the policy to the
target role:

```sh
aws iam attach-role-policy \
--role-name TargetRoleForS3Replication \
--policy-arn arn:aws:iam::779657758625:policy/TargetPolicyForS3Replication \
--endpoint http://node6:8600 \
--profile backbeat-target
```

### Create and configure buckets

Create the buckets:

```sh
aws s3api create-bucket \
--bucket source-bucket \
--endpoint http://node1 \
--profile backbeat-source && \
aws s3api create-bucket \
--bucket target-bucket \
--endpoint http://node6 \
--profile backbeat-target
```

Enable versioning on the buckets:

```sh
aws s3api put-bucket-versioning \
--bucket source-bucket \
--versioning-configuration Status=Enabled \
--endpoint http://node1 \
--profile backbeat-source && \
aws s3api put-bucket-versioning \
--bucket target-bucket \
--versioning-configuration Status=Enabled \
--endpoint http://node6 \
--profile backbeat-target
```

Now, using our role ARNs we will define a replication configuration for the
source bucket. In the configuration, the `Role` field takes the form of a comma
separated string `"<source-role-ARN>,<target-role-ARN>"`. See the example below.

Create a file replication-configuration.json with the following content:

```sh
{
    "Role": "arn:aws:iam::668546647514:role/SourceRoleForS3Replication,arn:aws:iam::779657758625:role/TargetRoleForS3Replication",
    "Rules": [
        {
            "Prefix": "",
            "Destination": {
                "Bucket": "arn:aws:s3:::target-bucket"
            },
            "Status": "Enabled"
        }
    ]
}
```

Set the replication configuration on the source bucket:

```sh
aws s3api put-bucket-replication \
--bucket source-bucket \
--replication-configuration file://replication-configuration.json \
--endpoint http://node1 \
--profile backbeat-source
```

### Replicate an object

Put an object to replicate:

```sh
aws s3api put-object \
--bucket source-bucket \
--key object-to-replicate \
--endpoint http://node1 \
--profile backbeat-source
```

After some time, the object should have the `ReplicationStatus` 'COMPLETED'.
Confirm that the object has been replicated:

```sh
aws s3api head-object \
--bucket source-bucket \
--key object-to-replicate \
--endpoint http://node1 \
--profile backbeat-source
```

Now the target object should have the `ReplicationStatus` 'REPLICA':

```sh
aws s3api head-object \
--bucket target-bucket \
--key object-to-replicate \
--endpoint http://node6 \
--profile backbeat-target
```
