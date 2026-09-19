export class Command {
  #name = '';
  #description = '';
  #version = '';
  #commands = new Map();
  #parent = null;
  #actionHandler = null;
  #options = [];

  name(value) {
    this.#name = value;
    return this;
  }

  description(value) {
    this.#description = value;
    return this;
  }

  version(value) {
    this.#version = value;
    return this;
  }

  command(name) {
    const child =
      new Command();

    child.#name = name;
    child.#parent = this;

    this.#commands.set(
      name,
      child
    );

    return child;
  }

  option(
    definition,
    description,
    defaultValue = undefined
  ) {
    const match =
      /^--([a-zA-Z0-9-]+)(?:\s+<[^>]+>)?$/
        .exec(definition);

    if (!match) {
      throw new TypeError(
        `invalid option definition: ${definition}`
      );
    }

    this.#options.push({
      name:
        match[1].replace(
          /-([a-z])/g,
          (_, letter) =>
            letter.toUpperCase()
        ),
      flag:
        `--${match[1]}`,
      description,
      defaultValue
    });

    return this;
  }

  action(handler) {
    if (typeof handler !== 'function') {
      throw new TypeError(
        'command action must be a function'
      );
    }

    this.#actionHandler =
      handler;

    return this;
  }

  async parse(argv) {
    const args =
      argv.slice(2);

    if (args.length === 0) {
      this.printHelp();
      return;
    }

    if (
      args[0] === '--help' ||
      args[0] === '-h'
    ) {
      this.printHelp();
      return;
    }

    if (
      args[0] === '--version' ||
      args[0] === '-v'
    ) {
      process.stdout.write(
        `${this.#version}\n`
      );
      return;
    }

    const command =
      this.#commands.get(
        args[0]
      );

    if (!command) {
      throw new Error(
        `unknown command: ${args[0]}`
      );
    }

    await command.#run(
      args.slice(1)
    );
  }

  async #run(args) {
    if (
      args[0] === '--help' ||
      args[0] === '-h'
    ) {
      this.printHelp();
      return;
    }

    if (
      args[0] === '--version' ||
      args[0] === '-v'
    ) {
      process.stdout.write(
        `${this.#version}\n`
      );
      return;
    }

    const values = {};

    for (const option of this.#options) {
      values[option.name] =
        option.defaultValue;
    }

    for (let i = 0; i < args.length; i++) {
      const arg =
        args[i];

      if (arg === '--help' || arg === '-h') {
        this.printHelp();
        return;
      }

      const option =
        this.#options.find(
          item =>
            item.flag === arg
        );

      if (!option) {
        throw new Error(
          `unknown option: ${arg}`
        );
      }

      const next =
        args[i + 1];

      if (
        next == null ||
        next.startsWith('--')
      ) {
        throw new Error(
          `missing value for ${arg}`
        );
      }

      values[option.name] =
        next;

      i++;
    }

    if (this.#actionHandler == null) {
      this.printHelp();
      return;
    }

    await this.#actionHandler(
      values
    );
  }

  printHelp() {
    const command =
      this;

    const lines = [
      command.#name,
      command.#description,
      ''
    ];

    if (command.#commands.size > 0) {
      lines.push(
        'Commands:',
        ...[...command.#commands.keys()].map(
          name =>
            `  ${name}`
        ),
        ''
      );
    }

    if (command.#options.length > 0) {
      lines.push(
        'Options:',
        ...command.#options.map(
          option =>
            `  ${option.flag}  ${option.description}`
        ),
        ''
      );
    }

    if (command.#version) {
      lines.push(
        '  -v, --version  Show version',
        ''
      );
    }

    process.stdout.write(
      lines.join('\n')
    );
  }
}
