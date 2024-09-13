# Regex Strings
## Kickoffs
- `(\#[0-9]+) ([a-zA-Z\.\-]+) kickoff ([0-9]+) yards to the ([0-9a-zA-Z]+) (\#[0-9]+) ([a-zA-Z\.\-]+) return ([0-9]+) yards to the ([0-9a-zA-Z]+)(\( (\#[0-9]+) ([a-zA-Z\.\-]+)\))?`

## Penalty
- `([A-Z{2|3}]+) Illegal sub \(too many men\)[ ]? ([\-0-9]+) yards from ([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)`
- `([A-Z{2|3}]+) Time count after 3min warning on 1st or 2nd down - Loss 10 yards \([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)`
- `([A-Z{2|3}]+) Time count after 3min warning on 1st or 2nd down - Loss of Down \([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)`
- `([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) to ([0-9a-zA-Z\-]+)`
- `([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) \([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) ([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)`
- `([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+) \([\#0-9]+ ([a-zA-Z\.\s\-\']+)\) ([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+)? to ([0-9a-zA-Z\-]+)`
- `([A-Z]{2,4}) ([\s\,a-zA-Z0-9\()]+)\s([\-0-9]+) yard[s]? from ([0-9a-zA-Z\-]+) to ([0-9a-zA-Z\-]+)`
- `([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+) declined`
- `([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+) , 1ST DOWN`
- `([A-Z]{2,4}) ([a-zA-Z\-\s\,0-9]+) \([\#0-9]+ ([a-zA-Z\.\s\-\']+)\)`

## Safety
- `\#0-9]+ ([a-zA-Z\.\-\s\']+) SAFETY TOUCH`
- ` ([a-zA-Z\.\-\s\']+) SAFETY TOUCH`