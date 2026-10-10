package transitsims.validation;

import java.util.ArrayList;
import java.util.List;

/** Task data of the GeneratedCode* doer methods; its history shows who got the same object after the loader. */
public class GeneratedCodeSecondData {
    final List<String> history = new ArrayList<>();

    public void touch(String by) {
        history.add(by);
    }

    @Override
    public String toString() {
        return "Second" + history;
    }
}
