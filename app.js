const { start, dispatch, stop, spawn, spawnStateless, children } = require('nact');
const fs = require('fs');

const resultFile = `IFF85_StasysS_dat.txt`;

const msgTypes = {
  //Skirstytuvo kodai
  INIT: "INIT",
  WORK: "WORK",
  //

  //Spausdintojo kodai
  PRINT_RESULTS: "PRINT_RESULTS",
  //

  //Darbuotojų kodai
  RESULT_OK: "RESULT_OK",
  RESULT_FAIL: "RESULT_FAIL",
  CALCULATE_BAC: "CALCULATE_BAC",
  //

  //Kaupiklio kodai
  PUT_RESULT: "PUT_RESULT",
  GET_RESULTS: "GET_RESULTS",
  //
};

const actorNames = {
  PRINTER: "PRINTER",
  ACCUMULATOR: "ACCUMULATOR",
};

// Spausdinimas
const spawnPrinter = (parent) => spawnStateless(
  parent, 
  (msg, ctx) => {
    if(msg.type === msgTypes.PRINT_RESULTS){
      const people = msg.data;

      const duomenuKiekis = `\nDuomęnų kiekis: ${msg.data.length}\n`
      const duomenuFailas = `Duomenų failas: IFF85_StasysS_dat_${msg.data.fileIndex}.json\n`

      const header = '____________________________________________________________\n|  #|Name                |Gender |Weight|Drinks|AC   |BAC  |\n|---+--------------------+-------+------+------+-----+-----|\n';
      const footer = '|__________________________________________________________|\n';

      const result = people.reduce((str, element, index) => {
        const indexas = (index+1).toString().padStart(3);
        const name = element.name.padEnd(20);
        const gender = element.gender.padEnd(7);
        const weight = element.weight.toString().padStart(6);
        const amountOfDrinks = element.amountOfDrinks.toString().padStart(6);
        const acOfDrinks = element.acOfDrinks.toString().padStart(5);
        const bac = element.BAC.toFixed(2).padStart(5);
        return `${str}|${indexas}|${name}|${gender}|${weight}|${amountOfDrinks}|${acOfDrinks}|${bac}|\n`;
      }, '');

      fs.appendFile(resultFile, duomenuKiekis+duomenuFailas+header+result+footer, err => {
        if (err) console.log(err);
      })  
    }
  },
  actorNames.PRINTER
);

// Darbuotojas
const spawnWorker = (parent, darbininkoID) => spawnStateless(
  parent, 
  (msg, ctx) => {
    if (msg.type === msgTypes.CALCULATE_BAC) {

      const person = msg.data;

      const gramsOfAlcohol = person.amountOfDrinks * person.acOfDrinks * 0.789;
      person.BAC = (gramsOfAlcohol / (person.weight * 1000 * (person.gender === 'female' ? 0.55 : 0.65 ))) * 1000;

      if(person.BAC > 0){
        dispatch(msg.sender, message(ctx.self, msgTypes.RESULT_OK, person));
      } else {
        dispatch(msg.sender, message(ctx.self, msgTypes.RESULT_FAIL));
      }    
    } else {
      dispatch(msg.sender, message(ctx.self, msgTypes.RESULT_FAIL));
    }
  },
  darbininkoID
);

// Kaupiklis
const spawnAccumulator = (parent) =>
  spawn(
    parent, 
    (state = {results: []}, msg, ctx) => {
      switch (msg.type) {
        
        case msgTypes.PUT_RESULT: {
          const people = state.results;
          const person = msg.data;
          const n = people.length;

          (n === 0) ? people.splice(0, 0, person) :
          people.forEach((element, index) => (element.name >= person.name && n === people.length) && people.splice(index, 0, person));

          //jei nebuvo pridėtas žmogus
          (n === people.length) && people.splice(n, 0, person);
          
          return {
            ...state, 
            results: people,
          };
        }

        case msgTypes.GET_RESULTS: {
          dispatch(msg.sender, message(ctx.self, msgTypes.PRINT_RESULTS, state.results));
          return state;
        }
        
        default: {
          return state;
        }
      }
    },
    actorNames.ACCUMULATOR
);

// Skirstytuvas
const spawnDistributor = (parent, n, itemCount, fileIndex) =>
  spawn(
    parent, 
    (state = { workersStack: [...Array(parseInt(n)).keys()], counter: 0 }, msg, ctx) => {
      switch (msg.type) {

        // Sukuriam visus likusius aktorius
        case msgTypes.INIT: {
          state.workersStack.forEach(id => {
            spawnWorker(ctx.self, id.toString());
          });
          spawnPrinter(ctx.self);
          spawnAccumulator(ctx.self);
          return state;
        }

        // Leidžiami darbininkai
        case msgTypes.WORK: {
          const stack = state.workersStack;

          if (stack.length === 0) {
            dispatch(ctx.self, message(msg.sender, msgTypes.WORK, msg.data));
          } else {
            const workerID = stack.pop();
            const worker = ctx.children.get(workerID.toString());
            dispatch(worker, message(ctx.self, msgTypes.CALCULATE_BAC, msg.data), ctx.self);
          }
          return {
            ...state,
            workersStack: stack,
          };
        }

        // Jei iš darbininko rezultatas atitiko filtrą
        case msgTypes.RESULT_OK: {
          const stack = state.workersStack;
          stack.push(msg.sender.name);

          const accumulator = ctx.children.get(actorNames.ACCUMULATOR);
          dispatch(accumulator, message(ctx.self, msgTypes.PUT_RESULT, msg.data));

          const counter = state.counter + 1;
          (counter === itemCount) && dispatch(accumulator, message(ctx.self, msgTypes.GET_RESULTS));

          return {
            ...state,
            workersStack: stack,
            counter: counter,
          };
        }

        // Jei iš darbininko rezultatas neatitiko filtro arba buvo siųsta kitokia žinutė
        case msgTypes.RESULT_FAIL: {
          const accumulator = ctx.children.get(actorNames.ACCUMULATOR);
          const stack = state.workersStack;
          stack.push(msg.sender.name);

          const counter = state.counter + 1;
          (counter === itemCount) && dispatch(accumulator, message(ctx.self, msgTypes.GET_RESULTS));

          return {
            ...state,
            workersStack: stack,
            counter: counter,
          };
        }

        // Kviečiamas rezultatų spausdinimas
        case msgTypes.PRINT_RESULTS: {
          const printer = ctx.children.get(actorNames.PRINTER);
          msg.data.fileIndex = fileIndex;
          dispatch(printer, message(ctx.self, msgTypes.PRINT_RESULTS, msg.data));

          return state;
        }

        default: {
          return state;
        }
      }
    },
);

// Kad žinutės visada turėtų vienodūs pavadinimus
const message = (sender, type, data) => {
  return {sender, type, data};
}

// Siunčia po vieną žmogų
const sendPeople = async (people, distributor, parent) => {
  people.forEach((person) =>
    dispatch(distributor, message(parent, msgTypes.WORK, person))
  );
};

// Skaičiavimai
const compute = () => {
  const system = start();

  fs.writeFile(resultFile, '', err => {
    (err) && console.log(err);
  })

  for (let i = 1; i <= 3; i++) {
    const people = require(`./data/IFF85_StasysS_dat_${i}.json`);
    const n = people.length;
    if (n === 0) continue;
    const distributor = spawnDistributor(system, Math.max(2, n / 4), people.length, i);
    dispatch(distributor, message(system, msgTypes.INIT));
    sendPeople(people, distributor, system);
  }
}

compute();
