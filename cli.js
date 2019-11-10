#!/usr/bin/env node

const path = require('path');
const os = require('os');
const program = require('commander');
const packageInfo = require('./package.json');
const kafka = require('./kafka');

const protocols = {
  kafka,
};

const red = text => `\x1b[31m${text}\x1b[0m`;
const magenta = text => `\x1b[35m${text}\x1b[0m`;
const yellow = text => `\x1b[33m${text}\x1b[0m`;
const green = text => `\x1b[32m${text}\x1b[0m`;

const showErrorAndExit = err => {
  console.error(red('Something went wrong:'));
  console.error(red(err.stack || err.message));
  if (err.errors) {
    console.error(red(JSON.stringify(err.errors)));
  }
  process.exit(1);
};

program
  .version(packageInfo.version)
  .option('-h, --host <host>', 'host where to find the broker')
  .option('-u, --username <username>', 'username to connect to the broker')
  .option('-p, --password <password>', 'password to connect to the broker')
  .option('-a, --auth <auth>', 'authentication mechanism to connect to the broker')
  ;

program
  .command('publish <protocol> <topic> <message>')
  .alias('pub');

  program
  .command('subscribe <protocol> <topic>')
  .alias('sub');

program.parse(process.argv);

const operation = program.args[0];
const protocol = program.args[1];
const channel = program.args[2];

if (!protocols[protocol]) showErrorAndExit(`Unsupported protocol: ${protocol}`);

if (operation === 'publish') {
  const message = program.args[3];
  protocols[protocol].publish({
    host: program.host,
    username: program.username,
    password: program.password,
    auth: program.auth,
    channel,
    message,
  });
} else if (operation === 'subscribe') {
  protocols[protocol].subscribe({
    host: program.host,
    username: program.username,
    password: program.password,
    auth: program.auth,
    channel,
  });
}

process.on('unhandledRejection', showErrorAndExit);
