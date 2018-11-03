const aws = require('aws-sdk');
const fs = require('fs');
const gm = require('gm').subClass({
  imageMagick: true
});
const config = require('./config');

exports.handler = async (event, context, callback) => {
  const s3 = new aws.S3();
  const sourceBucket = event.Records[0].s3.bucket.name;
  const objectKey = event.Records[0].s3.object.key;
  const getObjectParams = {
    Bucket: sourceBucket,
    Key: objectKey
  };
  try {
    console.log('get object from s3');
    const data = await s3.getObject(getObjectParams).promise();
    let promises = [];
    console.log('first push to queue');
    promises.push(pictureProcess(data, objectKey, null));
    for (const size of config.size) {
      console.log('loop: size ' + size);
      promises.push(pictureProcess(data, objectKey, size));
    }
    await Promise.all(promises);
    console.log('finish all tasks');
  } catch (error) {
    console.error(error);
  }

  async function pictureProcess(data, objectKey, size) {
    if (size === null) {
      const content = await compressPicture(data.Body, data.ContentType);
      return savePicture(objectKey, content, '', data.ContentType);
    } else {
      const content = await resizePicture(data.Body, size, data.ContentType);
      return savePicture(
        objectKey,
        content,
        size + 'x' + size + '/',
        data.ContentType
      );
    }
  }

  async function savePicture(objectKey, content, size, type) {
    await s3
      .upload({
        Bucket: config.destinationBucketName,
        Key: size + objectKey,
        Body: content,
        ContentType: type,
        ACL: 'public-read'
      })
      .promise();
  }

  function compressPicture(stream, type) {
    return new Promise((resolve, reject) => {
      gm(stream)
        .quality(config.quality)
        .toBuffer(type.split('/').pop(), function(err, buffer) {
          if (err) reject(err);
          resolve(buffer);
        });
    });
  }

  function resizePicture(stream, size, type) {
    return new Promise((resolve, reject) => {
      console.log('gm: size ' + size);
      gm(stream)
        .quality(config.quality)
        .resize(size, size)
        .toBuffer(type.split('/').pop(), function(err, buffer) {
          if (err) reject(err);
          resolve(buffer);
        });
    });
  }
};
