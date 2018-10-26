# Test Setup for the cgp-dss-data-loader
Some descriptions of how permissions were set and how to reproduce this.

## Base Loader: Optional Metadata Credentials Test Setup
This includes `test_base_loader.py` and `test_base_loader_integration.py`.

These require testing accounts that normally can't access (meta)data to show that 
they can access the (meta)data given the proper optional credentials.  This happens 
when we load protected access data if we want to see the size/hash/type.

Two (Google and AWS) buckets were created with the same name:
`travis-test-loader-dont-delete`

### AWS Setup
Two policies were created.  One to allow `AssumeRole` and the other to allow actions on the bucket.

POLICY (`TravisAssumeRoleLoader`):
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "sts:AssumeRole",
            "Resource": "arn:aws:iam::719818754276:role/travis_access_test_bucket"
        }
    ]
}
```


POLICY (`TravisBucketAccess`):
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "*",
            "Resource": [
                "arn:aws:s3:::travis-test-loader-dont-delete/*",
                "arn:aws:s3:::travis-test-loader-dont-delete"
            ]
        }
    ]
}
```

A role was created with the attached policy: `TravisBucketAccess` and a trust relationship 
with the travis account user and the creator of the policy.
ROLE (`travis_access_test_bucket`) TRUST RELATIONSHIP:
```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "AWS": [
          "arn:aws:iam::719818754276:user/travis_integration_test_CGP",
          "arn:aws:iam::719818754276:user/anon@ucsc.edu"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

A bucket was created with a restricted policy allowing only the role and the policy creator access.
BUCKET POLICY (travis-loader-test-dont-delete):
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "arn:aws:iam::719818754276:role/travis_access_test_bucket",
                    "arn:aws:sts::719818754276:assumed-role/travis_access_test_bucket/travis",
                    "arn:aws:iam::719818754276:user/anon@ucsc.edu"
                ]
            },
            "Action": "*",
            "Resource": [
                "arn:aws:s3:::travis-test-loader-dont-delete/*",
                "arn:aws:s3:::travis-test-loader-dont-delete"
            ]
        },
        {
            "Effect": "Deny",
            "NotPrincipal": {
                "AWS": [
                    "arn:aws:iam::719818754276:role/travis_access_test_bucket",
                    "arn:aws:sts::719818754276:assumed-role/travis_access_test_bucket/travis",
                    "arn:aws:iam::719818754276:user/anon@ucsc.edu"
                ]
            },
            "Action": "*",
            "Resource": [
                "arn:aws:s3:::travis-test-loader-dont-delete/*",
                "arn:aws:s3:::travis-test-loader-dont-delete"
            ]
        }
    ]
}
```

Finally, a group (`TravisAccess2TestBuckets`) was created with the attached 
policy `TravisAssumeRoleLoader` and the travis user and policy creator were added,
allowing those two users to assume the role that would allow them to access the bucket.

### Google Setup
The `travis-test-loader-dont-delete` bucket was created and the permissions deleted for 
`viewer` level users.

A service account with only viewer-level access was created: `travis-underpriveleged-tester@platform-dev-178517.iam.gserviceaccount.com` 
that now couldn't access the bucket.  Their permissions were generated as a json key 
through the browser.

A user, `travis.platform.dev@gmail.com`, was created with editor level permissions to be 
able to access the bucket.  Their permissions were generated with:

`gcloud auth application-default login`

And are stored in the AWS Secrets Manager under: `/travis/googlesecret/loadertestdontdelete`
